// Copyright 2020 Zhizhesihai (Beijing) Technology Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package zetta

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/zhihu/zetta-client-go/utils/retry"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"google.golang.org/grpc/codes"
)

//
// session 池配置
//
type SessionPoolConfig struct {
	// getRPCClient is the caller supplied method for getting a gRPC client to Cloud Spanner
	// this makes session pool able to use client pooling.
	// 返回可用的 RPC 客户端
	getNextDataRPCClient func() (tspb.TablestoreClient, error)
	// MaxOpened is the maximum number of opened sessions that is allowed by the
	// session pool, zero means unlimited.
	// 指定 session 池子的最大数量
	MaxOpened uint64
	// MinOpened is the minimum number of opened sessions that the session pool
	// tries to maintain. Session pool won't continue to expire sessions if number
	// of opened connections drops below MinOpened. However, if session is found
	// to be broken, it will still be evicted from session pool, therefore it is
	// posssible that the number of opened sessions drops below MinOpened.
	// 努力维持的最少数量
	MinOpened uint64
	// MaxSessionAge is the maximum duration that a session can be reused, zero
	// means session pool will never expire sessions.
	MaxSessionAge time.Duration
	// MaxBurst is the maximum number of concurrent session creation requests,
	MaxBurst uint64
	// WriteSessions is the fraction of sessions we try to keep prepared for write.
	WriteSessions float64
	// HealthCheckWorkers is number of workers used by health checker for this pool.
	HealthCheckWorkers int
	// HealthCheckInterval is how often the health checker pings a session.
	HealthCheckInterval time.Duration
}

// DefaultSessionPoolConfig is the default configuration for the session pool
// that will be used for a Spanner client, unless the user supplies a specific
// session pool config.
var DefaultSessionPoolConfig = SessionPoolConfig{
	MinOpened:           10,
	MaxOpened:           numChannels * 100,
	MaxBurst:            10,
	WriteSessions:       0.2,
	HealthCheckWorkers:  10,
	HealthCheckInterval: 5 * time.Minute,
}

// errMinOpenedGTMapOpened returns error for SessionPoolConfig.MaxOpened < SessionPoolConfig.MinOpened when SessionPoolConfig.MaxOpened is set.
func errMinOpenedGTMaxOpened(maxOpened, minOpened uint64) error {
	return zettaErrorf(codes.InvalidArgument,
		"require SessionPoolConfig.MaxOpened >= SessionPoolConfig.MinOpened, got %d and %d", maxOpened, minOpened)
}

// errWriteFractionOutOfRange returns error for
// SessionPoolConfig.WriteFraction < 0 or SessionPoolConfig.WriteFraction > 1
func errWriteFractionOutOfRange(writeFraction float64) error {
	return zettaErrorf(codes.InvalidArgument,
		"require SessionPoolConfig.WriteSessions >= 0.0 && SessionPoolConfig.WriteSessions <= 1.0, got %.2f", writeFraction)
}

// errHealthCheckWorkersNegative returns error for
// SessionPoolConfig.HealthCheckWorkers < 0
func errHealthCheckWorkersNegative(workers int) error {
	return zettaErrorf(codes.InvalidArgument,
		"require SessionPoolConfig.HealthCheckWorkers >= 0, got %d", workers)
}

// errHealthCheckIntervalNegative returns error for
// SessionPoolConfig.HealthCheckInterval < 0
func errHealthCheckIntervalNegative(interval time.Duration) error {
	return zettaErrorf(codes.InvalidArgument,
		"require SessionPoolConfig.HealthCheckInterval >= 0, got %v", interval)
}

// validate verifies that the SessionPoolConfig is good for use.
func (spc *SessionPoolConfig) validate() error {
	if spc.getNextDataRPCClient == nil {
		return errors.New("invalid session pool config")
	}
	if spc.MinOpened > spc.MaxOpened && spc.MaxOpened > 0 {
		return fmt.Errorf("invalid pool size out of [%d, %d]", spc.MinOpened, spc.MaxOpened)
	}

	if spc.WriteSessions < 0.0 || spc.WriteSessions > 1.0 {
		return errWriteFractionOutOfRange(spc.WriteSessions)
	}
	if spc.HealthCheckWorkers < 0 {
		return errHealthCheckWorkersNegative(spc.HealthCheckWorkers)
	}
	if spc.HealthCheckInterval < 0 {
		return errHealthCheckIntervalNegative(spc.HealthCheckInterval)
	}
	return nil
}

//
// sessionPool creates and caches Cloud Spanner sessions.
//
type sessionPool struct {
	// mu protects sessionPool from concurrent access.
	mu sync.Mutex
	// valid marks the validity of the session pool.
	valid bool
	// db is the database name that all sessions in the pool are associated with.
	// session 池里的 session 和某个具体的数据块是绑定的
	db string
	// idleList caches idle session IDs. Session IDs in this list can be allocated for use.
	// 闲置链表
	idleList list.List
	// idleWriteList caches idle sessions which have been prepared for write.
	// 写准备好链表
	idleWriteList list.List
	// mayGetSession is for broadcasting that session retrival/creation may proceed.
	mayGetSession chan struct{}
	// numOpened is the total number of open sessions from the session pool.
	numOpened uint64
	// createReqs is the number of ongoing session creation requests.
	createReqs uint64
	// prepareReqs is the number of ongoing session preparation request.
	prepareReqs uint64
	// configuration of the session pool.
	SessionPoolConfig
	// hc is the health checker
	hc *healthChecker
}

// newSessionPool creates a new session pool.
func newSessionPool(db string, config SessionPoolConfig) (*sessionPool, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	pool := &sessionPool{
		db:                db,
		valid:             true,
		mayGetSession:     make(chan struct{}),
		SessionPoolConfig: config,
	}
	if config.HealthCheckWorkers == 0 {
		// With 10 workers and assuming average latency of 5 ms for BeginTransaction, we will be able to
		// prepare 2000 tx/sec in advance. If the rate of takeWriteSession is more than that, it will
		// degrade to doing BeginTransaction inline.
		// TODO: consider resizing the worker pool dynamically according to the load.
		config.HealthCheckWorkers = 10
	}
	if config.HealthCheckInterval == 0 {
		config.HealthCheckInterval = 5 * time.Minute
	}
	// On GCE VM, within the same region an healthcheck ping takes on average 10ms to finish, given a 5 minutes interval and
	// 10 healthcheck workers, a healthChecker can effectively mantain 100 checks_per_worker/sec * 10 workers * 300 seconds = 300K sessions.
	pool.hc = newHealthChecker(config.HealthCheckInterval, config.HealthCheckWorkers, pool)
	return pool, nil
}

// isValid checks if the session pool is still valid.
func (p *sessionPool) isValid() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.valid
}

// close marks the session pool as closed.
func (p *sessionPool) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if !p.valid {
		p.mu.Unlock()
		return
	}
	p.valid = false
	p.mu.Unlock()
	p.hc.close()
	// destroy all the sessions
	p.hc.mu.Lock()
	allSessions := make([]*session, len(p.hc.queue.sessions))
	copy(allSessions, p.hc.queue.sessions)
	p.hc.mu.Unlock()
	for _, s := range allSessions {
		s.destroy(false)
	}
}

// shouldPrepareWrite returns true if we should prepare more sessions for write.
func (p *sessionPool) shouldPrepareWrite() bool {
	return float64(p.numOpened)*p.WriteSessions > float64(p.idleWriteList.Len()+int(p.prepareReqs))
}

func (p *sessionPool) createSession(ctx context.Context) (*session, error) {
	doneCreate := func(done bool) {
		p.mu.Lock()
		if !done {
			// Session creation failed, give budget back.
			p.numOpened--
		}
		p.createReqs--
		// Notify other waiters blocking on session creation.
		close(p.mayGetSession)
		p.mayGetSession = make(chan struct{})
		p.mu.Unlock()
	}
	dc, err := p.getNextDataRPCClient()
	if err != nil {
		doneCreate(false)
		return nil, err
	}
	var s *session
	err = retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		sid, e := dc.CreateSession(ctx, &tspb.CreateSessionRequest{Database: p.db})
		if e != nil {
			return e
		}
		s = &session{
			db:         p.db,
			client:     dc,
			id:         sid.Name,
			createTime: time.Now(),
			mu:         sync.Mutex{},
			valid:      true,
			pool:       p,
			tx:         nil,
		}
		p.hc.register(s)
		return nil
	})
	if err != nil {
		doneCreate(false)
		// Should return error directly because of the previous retries on CreateSession RPC.
		return nil, err
	}
	doneCreate(true)
	return s, nil
}

func (p *sessionPool) isHealthy(s *session) bool {
	if s.getNextCheck().Add(2 * p.hc.getInterval()).Before(time.Now()) {
		// TODO: figure out if we need to schedule a new healthcheck worker here.
		if err := s.ping(); shouldDropSession(err) {
			// The session is already bad, continue to fetch/create a new one.
			s.destroy(false)
			return false
		}
		p.hc.scheduledHC(s)
	}
	return true
}

// take returns a cached session if there are available ones; if there isn't any, it tries to allocate a new one.
// Session returned by take should be used for read operations.
func (p *sessionPool) take(ctx context.Context) (*sessionHandle, error) {
	for {
		var (
			s   *session
			err error
		)

		p.mu.Lock()
		if !p.valid {
			p.mu.Unlock()
			return nil, ERR_SESSION_POOL_INVALID
		}
		if p.idleList.Len() > 0 {
			// Idle sessions are available, get one from the top of the idle list.
			s = p.idleList.Remove(p.idleList.Front()).(*session)
		} else if p.idleWriteList.Len() > 0 {
			s = p.idleWriteList.Remove(p.idleWriteList.Front()).(*session)
		}
		if s != nil {
			s.setIdleList(nil)
			p.mu.Unlock()
			// From here, session is no longer in idle list, so healthcheck workers won't destroy it.
			// If healthcheck workers failed to schedule healthcheck for the session timely, do the check here.
			// Because session check is still much cheaper than session creation, they should be reused as much as possible.
			if !p.isHealthy(s) {
				continue
			}
			return &sessionHandle{session: s}, nil
		}
		// Idle list is empty, block if session pool has reached max session creation concurrency or max number of open sessions.
		if (p.MaxOpened > 0 && p.numOpened >= p.MaxOpened) || (p.MaxBurst > 0 && p.createReqs >= p.MaxBurst) {
			mayGetSession := p.mayGetSession
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ERR_SESSION_POOL_TIMEOUT
			case <-mayGetSession:
			}
			continue
		}
		// Take budget before the actual session creation.
		p.numOpened++
		p.createReqs++
		p.mu.Unlock()
		if s, err = p.createSession(ctx); err != nil {
			return nil, err
		}
		return &sessionHandle{session: s}, nil
	}
}

// takeWriteSession returns a write prepared cached session if there are available ones; if there isn't any, it tries to allocate a new one.
// Session returned should be used for read write transactions.
func (p *sessionPool) takeWriteSession(ctx context.Context) (*sessionHandle, error) {
	for {
		var (
			s   *session
			err error
		)

		p.mu.Lock()
		if !p.valid {
			p.mu.Unlock()
			return nil, ERR_SESSION_POOL_INVALID
		}
		if p.idleWriteList.Len() > 0 {
			// Idle sessions are available, get one from the top of the idle list.
			s = p.idleWriteList.Remove(p.idleWriteList.Front()).(*session)
		} else if p.idleList.Len() > 0 {
			s = p.idleList.Remove(p.idleList.Front()).(*session)
		}
		if s != nil {
			s.setIdleList(nil)
			p.mu.Unlock()
			// From here, session is no longer in idle list, so healthcheck workers won't destroy it.
			// If healthcheck workers failed to schedule healthcheck for the session timely, do the check here.
			// Because session check is still much cheaper than session creation, they should be reused as much as possible.
			if !p.isHealthy(s) {
				continue
			}
			if !s.isWritePrepared() {
				if err = s.prepareForWrite(ctx); err != nil {
					return nil, err
				}
			}
			return &sessionHandle{session: s}, nil
		}
		// Idle list is empty, block if session pool has reached max session creation concurrency or max number of open sessions.
		if (p.MaxOpened > 0 && p.numOpened >= p.MaxOpened) || (p.MaxBurst > 0 && p.createReqs >= p.MaxBurst) {
			mayGetSession := p.mayGetSession
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ERR_SESSION_POOL_TIMEOUT
			case <-mayGetSession:
			}
			continue
		}

		// Take budget before the actual session creation.
		p.numOpened++
		p.createReqs++
		p.mu.Unlock()
		if s, err = p.createSession(ctx); err != nil {
			return nil, err
		}
		if err = s.prepareForWrite(ctx); err != nil {
			return nil, err
		}
		return &sessionHandle{session: s}, nil
	}
}

// recycle puts session s back to the session pool's idle list, it returns true if the session pool successfully recycles session s.
func (p *sessionPool) recycle(s *session) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !s.isValid() || !p.valid {
		// Reject the session if session is invalid or pool itself is invalid.
		return false
	}
	if p.MaxSessionAge != 0 && s.createTime.Add(p.MaxSessionAge).Before(time.Now()) && p.numOpened > p.MinOpened {
		// session expires and number of opened sessions exceeds MinOpened, let the session destroy itself.
		return false
	}
	// Hot sessions will be converging at the front of the list, cold sessions will be evicted by healthcheck workers.
	if s.isWritePrepared() {
		s.setIdleList(p.idleWriteList.PushFront(s))
	} else {
		s.setIdleList(p.idleList.PushFront(s))
	}
	// Broadcast that a session has been returned to idle list.
	close(p.mayGetSession)
	p.mayGetSession = make(chan struct{})
	return true
}

// remove atomically removes session s from the session pool and invalidates s.
// If isExpire == true, the removal is triggered by session expiration and in such cases, only idle sessions can be removed.
func (p *sessionPool) remove(s *session, isExpire bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if isExpire && (p.numOpened <= p.MinOpened || s.getIdleList() == nil) {
		// Don't expire session if the session is not in idle list (in use), or if number of open sessions is going below p.MinOpened.
		return false
	}
	ol := s.setIdleList(nil)
	// If the session is in the idlelist, remove it.
	if ol != nil {
		// Remove from whichever list it is in.
		p.idleList.Remove(ol)
		p.idleWriteList.Remove(ol)
	}
	if s.invalidate() {
		// Decrease the number of opened sessions.
		p.numOpened--
		// Broadcast that a session has been destroyed.
		close(p.mayGetSession)
		p.mayGetSession = make(chan struct{})
		return true
	}
	return false
}
