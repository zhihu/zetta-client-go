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
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

// KeySet is collection of keys and/or key ranges
// all the keys in the same table or index. and keys needn't be sorted
type KeySet struct {
	// true means all rows of a table or under a index.
	All bool
	// a list of keys covered by KeySet
	Keys []Key
	// list of key ranges covered by KeySet
	Ranges []KeyRange
}

// AllKeys returns a KeySet that represents all Keys of a table or a index.
func AllKeys() KeySet {
	return KeySet{All: true}
}

// Keys returns a KeySet for a set of keys.
func Keys(keys ...Key) KeySet {
	ks := KeySet{Keys: make([]Key, len(keys))}
	copy(ks.Keys, keys)
	return ks
}

// Range returns a KeySet for a range of keys.
func Range(r KeyRange) KeySet {
	return KeySet{Ranges: []KeyRange{r}}
}

// PrefixRange returns a KeySet for all keys with the given prefix, which is a key itself
func PrefixRange(prefix Key) KeySet {
	return KeySet{Ranges: []KeyRange{
		{
			Start: prefix,
			End:   prefix,
			Kind:  ClosedClosed,
		},
	}}
}

// union key set to one unity
func UnionKeySets(keySets ...KeySet) KeySet {
	s := KeySet{}
	for _, ks := range keySets {
		if ks.All {
			return KeySet{All: true}
		}
		s.Keys = append(s.Keys, ks.Keys...)
		s.Ranges = append(s.Ranges, ks.Ranges...)
	}
	return s
}

// proto converts KeySet into tspb.KeySet, which is the protobuf
// representation of KeySet.
func (keys KeySet) proto() (*tspb.KeySet, error) {
	pb := &tspb.KeySet{
		Keys:   make([]*tspb.ListValue, 0, len(keys.Keys)),
		Ranges: make([]*tspb.KeyRange, 0, len(keys.Ranges)),
		All:    keys.All,
	}
	for _, key := range keys.Keys {
		keyProto, err := key.proto()
		if err != nil {
			return nil, err
		}
		pb.Keys = append(pb.Keys, keyProto)
	}
	for _, r := range keys.Ranges {
		rProto, err := r.proto()
		if err != nil {
			return nil, err
		}
		pb.Ranges = append(pb.Ranges, rProto)
	}
	return pb, nil
}
