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
	"container/heap"
	"context"
	"math/rand"
	"sync"
	"time"
)

// hcHeap implements heap.Interface. It is used to create the priority queue for session healthchecks.
type hcHeap struct {
	sessions []*session
}

// Len impelemnts heap.Interface.Len.
func (h hcHeap) Len() int {
	return len(h.sessions)
}

// Less implements heap.Interface.Less.
func (h hcHeap) Less(i, j int) bool {
	return h.sessions[i].getNextCheck().Before(h.sessions[j].getNextCheck())
}

// Swap implements heap.Interface.Swap.
func (h hcHeap) Swap(i, j int) {
	h.sessions[i], h.sessions[j] = h.sessions[j], h.sessions[i]
	h.sessions[i].setHcIndex(i)
	h.sessions[j].setHcIndex(j)
}

// Push implements heap.Interface.Push.
func (h *hcHeap) Push(s interface{}) {
	ns := s.(*session)
	ns.setHcIndex(len(h.sessions))
	h.sessions = append(h.sessions, ns)
}

// Pop implements heap.Interface.Pop.
func (h *hcHeap) Pop() interface{} {
	old := h.sessions
	n := len(old)
	s := old[n-1]
	h.sessions = old[:n-1]
	s.setHcIndex(-1)
	return s
}

//
// healthChecker performs periodical healthchecks on registered sessions.
//
type healthChecker struct {
	// mu protects concurrent access to hcQueue.
	mu sync.Mutex
	// queue is the priority queue for session healthchecks. Sessions with lower nextCheck rank higher in the queue.
	queue hcHeap
	// interval is the average interval between two healthchecks on a session.
	interval time.Duration
	// workers is the number of concurrent healthcheck workers.
	workers int
	// waitWorkers waits for all healthcheck workers to exit
	waitWorkers sync.WaitGroup
	// pool is the underlying session pool.
	pool *sessionPool
	// closed marks if a healthChecker has been closed.
	closed bool
}

// newHealthChecker initializes new instance of healthChecker.
func newHealthChecker(interval time.Duration, workers int, pool *sessionPool) *healthChecker {
	if workers <= 0 {
		workers = 1
	}
	hc := &healthChecker{
		interval: interval,
		workers:  workers,
		pool:     pool,
	}
	for i := 0; i < hc.workers; i++ {
		hc.waitWorkers.Add(1)
		go hc.worker(i)
	}
	return hc
}

// close closes the healthChecker and waits for all healthcheck workers to exit.
func (hc *healthChecker) close() {
	hc.mu.Lock()
	hc.closed = true
	hc.mu.Unlock()
	hc.waitWorkers.Wait()
}

// isClosing checks if a healthChecker is already closing.
func (hc *healthChecker) isClosing() bool {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	return hc.closed
}

// getInterval gets the healthcheck interval.
func (hc *healthChecker) getInterval() time.Duration {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	return hc.interval
}

// scheduledHCLocked schedules next healthcheck on session s with the assumption that hc.mu is being held.
func (hc *healthChecker) scheduledHCLocked(s *session) {
	// The next healthcheck will be scheduled after [interval*0.5, interval*1.5) nanoseconds.
	nsFromNow := rand.Int63n(int64(hc.interval)) + int64(hc.interval)/2
	s.setNextCheck(time.Now().Add(time.Duration(nsFromNow)))
	if hi := s.getHcIndex(); hi != -1 {
		// Session is still being tracked by healthcheck workers.
		heap.Fix(&hc.queue, hi)
	}
}

// scheduledHC schedules next healthcheck on session s. It is safe to be called concurrently.
func (hc *healthChecker) scheduledHC(s *session) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.scheduledHCLocked(s)
}

// register registers a session with healthChecker for periodical healthcheck.
func (hc *healthChecker) register(s *session) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.scheduledHCLocked(s)
	heap.Push(&hc.queue, s)
}

// unregister unregisters a session from healthcheck queue.
func (hc *healthChecker) unregister(s *session) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	oi := s.setHcIndex(-1)
	if oi >= 0 {
		heap.Remove(&hc.queue, oi)
	}
}

// markDone marks that health check for session has been performed.
func (hc *healthChecker) markDone(s *session) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	s.checkingHealth = false
}

// healthCheck checks the health of the session and pings it if needed.
func (hc *healthChecker) healthCheck(s *session) {
	defer hc.markDone(s)
	if !s.pool.isValid() {
		// Session pool is closed, perform a garbage collection.
		s.destroy(false)
		return
	}
	if s.pool.MaxSessionAge != 0 && s.createTime.Add(s.pool.MaxSessionAge).Before(time.Now()) {
		// Session reaches its maximum age, retire it. Failing that try to refresh it.
		if s.destroy(true) || !s.refreshIdle() {
			return
		}
	}
	if err := s.ping(); shouldDropSession(err) {
		// Ping failed, destroy the session.
		s.destroy(false)
	}
}

// worker performs the healthcheck on sessions in healthChecker's priority queue.
func (hc *healthChecker) worker(i int) {
	// log.Printf("Starting health check worker %v", i)
	// Returns a session which we should ping to keep it alive.
	getNextForPing := func() *session {
		hc.pool.mu.Lock()
		defer hc.pool.mu.Unlock()
		hc.mu.Lock()
		defer hc.mu.Unlock()
		if hc.queue.Len() <= 0 {
			// Queue is empty.
			return nil
		}
		s := hc.queue.sessions[0]
		if s.getNextCheck().After(time.Now()) && hc.pool.valid {
			// All sessions have been checked recently.
			return nil
		}
		hc.scheduledHCLocked(s)
		if !s.checkingHealth {
			s.checkingHealth = true
			return s
		}
		return nil
	}

	// Returns a session which we should prepare for write.
	getNextForTx := func() *session {
		hc.pool.mu.Lock()
		defer hc.pool.mu.Unlock()
		if hc.pool.shouldPrepareWrite() {
			if hc.pool.idleList.Len() > 0 && hc.pool.valid {
				hc.mu.Lock()
				defer hc.mu.Unlock()
				if hc.pool.idleList.Front().Value.(*session).checkingHealth {
					return nil
				}
				session := hc.pool.idleList.Remove(hc.pool.idleList.Front()).(*session)
				session.checkingHealth = true
				hc.pool.prepareReqs++
				return session
			}
		}
		return nil
	}

	for {
		if hc.isClosing() {
			// log.Printf("Closing health check worker %v", i)
			// Exit when the pool has been closed and all sessions have been destroyed
			// or when health checker has been closed.
			hc.waitWorkers.Done()
			return
		}
		ws := getNextForTx()
		if ws != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ws.prepareForWrite(ctx)
			hc.pool.recycle(ws)
			hc.pool.mu.Lock()
			hc.pool.prepareReqs--
			hc.pool.mu.Unlock()
			hc.markDone(ws)
		}
		rs := getNextForPing()
		if rs == nil {
			if ws == nil {
				// No work to be done so sleep to avoid burning cpu
				pause := int64(100 * time.Millisecond)
				if pause > int64(hc.interval) {
					pause = int64(hc.interval)
				}
				<-time.After(time.Duration(rand.Int63n(pause) + pause/2))
			}
			continue
		}
		hc.healthCheck(rs)
	}
}
