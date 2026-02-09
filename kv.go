package kvdb

import (
	"bytes"
	"io"
	"slices"
)

type KV struct {
	log Log
	mem map[string][]byte // TODO: Remove
	// * Refactoring from map to slices so I can implement comparison operations later
	// * future note: currently all of the DB data needs to be loaded into memory ~ need to read up on ways to only have partial memory loaded
	keys [][]byte
	vals [][]byte 
}

type updateMode int 

const (
	ModeUpsert updateMode = 0 // insert OR update
	ModeInsert updateMode = 1 // insert NEW
	ModeUpdate updateMode = 2 // update EXISTING
)

func (kv *KV) Open() error {
	// TODO: Rework
	if err := kv.log.Open(); err != nil {
		return err
	}
	kv.mem = map[string][]byte{}
	
	for {
		entry := Entry{}
		eof, err := kv.log.Read(&entry)
		
		if eof || err == ErrBadSum || err == io.ErrUnexpectedEOF { break }
		if err != nil { return err}
		
		// entry go created/updated OR deleted
		if entry.deleted {
			delete(kv.mem, string(entry.key))
		} else {
			kv.mem[string(entry.key)] = entry.val
		}
	}

	return nil
}

func (kv *KV) Close() error { return kv.log.Close() }

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	if idx, found := BinarySearchFunc(kv.keys,key,bytes.Compare); found {
		return kv.vals[idx], found, nil
	}
	return nil, false, nil 
}

func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	return kv.SetEx(key,val,ModeUpsert)
}

func (kv *KV) Del(key []byte) (deleted bool, err error) {
	if idx, found := BinarySearchFunc(kv.keys,key,bytes.Compare); found {
		if err = kv.log.Write(&Entry{key: key, deleted: true}); err != nil {
			return false, err 
		}
		kv.keys = slices.Delete(kv.keys,idx,idx+1)
		kv.vals = slices.Delete(kv.vals,idx,idx+1)
		return true, nil 
	}
	return false, nil
}

func (kv *KV) SetEx(key []byte, val []byte, mode updateMode) (updating bool, err error) {
	idx, existed := BinarySearchFunc(kv.keys,key,bytes.Compare)
	// TODO: Rework
	prevVal, existed := kv.mem[string(key)]
	switch mode{
	case ModeUpsert:
		updating = !existed || !bytes.Equal(prevVal,val)
	case ModeInsert:
		updating = !existed
	case ModeUpdate:
		updating = existed && !bytes.Equal(prevVal,val)
	default:
		panic("NOT A VALID UPDATE MODE")
	}
	if updating {
			if err := kv.log.Write(&Entry{key: key,val: val, deleted: false}); err != nil {
				return false, err 
			}
			kv.mem[string(key)] = val
	}
	return updating, nil
}