// Package kvdb implements a persistent key-value store with
// relational table abstractions and order preserving serialization
package kvdb

import (
	"bytes"
	"io"
	"slices"
)

// KV represents the core storage engine.
// It maintains a sorted in-memory index and manages the Write-Ahead Log (WAL) for durability.
type KV struct {
	log Log
	// * future note: currently all of the DB data needs to be loaded into memory ~ need to read up on ways to only have partial memory loaded
	keys [][]byte
	vals [][]byte
}

// KVIterator provides a stateful cursor for traversing the sorted
// key-value pairs in the database.
type KVIterator struct {
	keys [][]byte
	vals [][]byte
	pos  int
}

// RangedKVIter wraps a KVIterator to provide a cursor for traversing
// key-value pairs within a specific byte range.
type RangedKVIter struct {
	iter KVIterator
	stop []byte
	desc bool
}

type updateMode int

const (
	ModeUpsert updateMode = 0 // insert OR update
	ModeInsert updateMode = 1 // insert NEW
	ModeUpdate updateMode = 2 // update EXISTING
)

// Open initializes the storage engine and reconstructs the in-memory state by replaying the Write-Ahead Log.
func (kv *KV) Open() error {

	if err := kv.log.Open(); err != nil {
		return err
	}
	// neat trick to reuse existing memory
	kv.keys = kv.keys[:0]
	kv.vals = kv.vals[:0]

	entries := []Entry{}

	for {
		entry := Entry{}
		eof, err := kv.log.Read(&entry)

		if eof || err == ErrBadSum || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return err
		}

		entries = append(entries, entry)
	}

	// groups together the same key operations
	slices.SortStableFunc(entries, func(a Entry, b Entry) int {
		return bytes.Compare(a.key, b.key)
	})

	for _, entry := range entries {
		n := len(kv.keys)
		if n > 0 && bytes.Equal(entry.key, kv.keys[n-1]) {
			//remove current entry found for the key found an updated one
			kv.keys = kv.keys[:n-1]
			kv.vals = kv.vals[:n-1]
		}

		if !entry.deleted {
			kv.keys = append(kv.keys, entry.key)
			kv.vals = append(kv.vals, entry.val)
		}
	}
	return nil
}

// Close terminates the connection between the KV and the log file.
func (kv *KV) Close() error { return kv.log.Close() }

// Get retrieves the value associated with a key. It reports whether the key exists in the store.
func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	if idx, found := BinarySearchFunc(kv.keys, key, bytes.Compare); found {
		return kv.vals[idx], found, nil
	}
	return nil, false, nil
}

// Set adds the value associated with a key. It reports whether the update occurred.
func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	return kv.SetEx(key, val, ModeUpsert)
}

// Del removes a key and its value from the store. It reports whether a deletion occurred.
func (kv *KV) Del(key []byte) (deleted bool, err error) {
	if idx, found := BinarySearchFunc(kv.keys, key, bytes.Compare); found {
		if err = kv.log.Write(&Entry{key: key, deleted: true}); err != nil {
			return false, err
		}
		kv.keys = slices.Delete(kv.keys, idx, idx+1)
		kv.vals = slices.Delete(kv.vals, idx, idx+1)
		return true, nil
	}
	return false, nil
}

// SetEx performs a conditional mutation on the store based on the provided updateMode.
// It reports whether the store was modified.
func (kv *KV) SetEx(key []byte, val []byte, mode updateMode) (updating bool, err error) {
	idx, existed := BinarySearchFunc(kv.keys, key, bytes.Compare)

	switch mode {
	case ModeUpsert:
		updating = !existed || !bytes.Equal(kv.vals[idx], val)
	case ModeInsert:
		updating = !existed
	case ModeUpdate:
		updating = existed && !bytes.Equal(kv.vals[idx], val)
	default:
		panic("NOT A VALID UPDATE MODE")
	}
	if updating {
		if err := kv.log.Write(&Entry{key: key, val: val, deleted: false}); err != nil {
			return false, err
		}
		if existed {
			kv.vals[idx] = val
		} else {
			kv.keys = slices.Insert(kv.keys, idx, key)
			kv.vals = slices.Insert(kv.vals, idx, val)
		}
	}
	return updating, nil
}

// Seek returns a KVIterator positioned at the first key greater than or equal to the key.
func (kv *KV) Seek(key []byte) (*KVIterator, error) {
	idx, _ := BinarySearchFunc(kv.keys, key, bytes.Compare)
	return &KVIterator{keys: kv.keys, vals: kv.vals, pos: idx}, nil
}

// Valid reports whether the iterator is currently positioned at a valid key-value pair.
func (iter *KVIterator) Valid() bool {
	return 0 <= iter.pos && iter.pos < len(iter.keys)
}

// Key returns the key at the current KVIterator position.
func (iter *KVIterator) Key() []byte {
	return iter.keys[iter.pos]
}

// Val returns the value at the current KVIterator position.
func (iter *KVIterator) Val() []byte {
	return iter.vals[iter.pos]
}

// Next moves the iterator cursor to the next key-value pair.
func (iter *KVIterator) Next() error {
	if iter.pos < len(iter.keys) {
		iter.pos += 1
	}
	return nil
}

// Prev moves the iterator cursor to the previous key-value pair.
func (iter *KVIterator) Prev() error {
	if iter.pos >= 0 {
		iter.pos -= 1
	}
	return nil
}

// Key returns the key at the current iterator position.
func (iter *RangedKVIter) Key() []byte {
	return iter.iter.Key()
}

// Val returns the value at the current iterator position.
func (iter *RangedKVIter) Val() []byte {
	return iter.iter.Val()
}

// Valid reports whether the current position is within the defined range.
// It returns false if the underlying iterator is exhausted or if the
// current key has passed the stopping boundary.
func (iter *RangedKVIter) Valid() bool {
	if !iter.iter.Valid() {
		return false
	}

	r := bytes.Compare(iter.iter.Key(), iter.stop)
	if iter.desc && r < 0 {
		return false
	} else if !iter.desc && r > 0 {
		return false
	}
	return true
}

// Next advances the cursor to the next valid key in the defined search order.
func (iter *RangedKVIter) Next() error {
	if !iter.Valid() {
		return nil
	}
	if iter.desc {
		return iter.iter.Prev()
	} else {
		return iter.iter.Next()
	}
}

// Range returns a RangedKVIter configured to traverse keys between the
// start and stop boundaries.
//
// If desc is false, it traverses start <= key <= stop.
// If desc is true, it traverses start >= key >= stop.
func (kv *KV) Range(start, stop []byte, desc bool) (*RangedKVIter, error) {
	iter, err := kv.Seek(start)
	if err != nil {
		return nil, err
	}

	// For descending scans, if Seek landed on a key strictly greater
	// than the start, move back one position to the largest valid key.
	if desc && (!iter.Valid() || bytes.Compare(iter.Key(), start) > 0) {
		if err := iter.Prev(); err != nil {
			return nil, err
		}
	}

	return &RangedKVIter{iter: *iter, stop: stop, desc: desc}, nil
}
