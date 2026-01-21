package kvdb

import (
	"bytes"
	"io"
)

type KV struct {
	log Log
	mem map[string][]byte
}


func (kv *KV) Open() error {
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
	val, ok = kv.mem[string(key)]
	return
}

func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	prevVal, existed := kv.mem[string(key)]
	updated = !existed || !bytes.Equal(prevVal,val)
	if updated {
		if err = kv.log.Write(&Entry{key: key,val: val, deleted: false}); err != nil {
			return false, err 
		}
		kv.mem[string(key)] = val
	}
	return
}

func (kv *KV) Del(key []byte) (deleted bool, err error) {
	_, deleted = kv.mem[string(key)]
	if deleted {
		if err = kv.log.Write(&Entry{key: key, deleted: true}); err != nil {
			return false, err 
		}
		delete(kv.mem, string(key))
	}
	return

}