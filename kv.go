package kvdb

import (
	"bytes"
	"io"
	"slices"
)

type KV struct {
	log Log
	// * future note: currently all of the DB data needs to be loaded into memory ~ need to read up on ways to only have partial memory loaded
	keys [][]byte
	vals [][]byte
}

type KVIterator struct {
	keys [][]byte
	vals [][]byte
	pos  int
}

type updateMode int

const (
	ModeUpsert updateMode = 0 // insert OR update
	ModeInsert updateMode = 1 // insert NEW
	ModeUpdate updateMode = 2 // update EXISTING
)

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

func (kv *KV) Close() error { return kv.log.Close() }

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	if idx, found := BinarySearchFunc(kv.keys, key, bytes.Compare); found {
		return kv.vals[idx], found, nil
	}
	return nil, false, nil
}

func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	return kv.SetEx(key, val, ModeUpsert)
}

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

func (kv *KV) Seek(key []byte) (*KVIterator, error) {
	idx, _ := BinarySearchFunc(kv.keys,key,bytes.Compare)
	return &KVIterator{keys: kv.keys, vals: kv.vals, pos: idx}, nil 
}

func (iter *KVIterator) Valid() bool {
	return 0 <= iter.pos && iter.pos < len(iter.keys)
}

func (iter *KVIterator) Key() []byte {
	return iter.keys[iter.pos]
}
func(iter *KVIterator) Val() []byte {
	return iter.vals[iter.pos]
}

func(iter *KVIterator) Next() error {
	if iter.pos < len(iter.keys) {
		iter.pos += 1
	}
	return nil 
}
func(iter *KVIterator) Prev() error {
	if iter.pos >= 0 {
		iter.pos -= 1
	}
	return nil 
}