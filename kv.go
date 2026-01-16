package kvdb

import "bytes"

type KV struct {
	mem map[string][]byte // []byte is unsuported as key to maps in GO
}

func (kv *KV) Open() error {
	kv.mem = map[string][]byte{} //empty
	return nil
}

func (kv *KV) Close() error { return nil }

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	val, ok = kv.mem[string(key)]
	return
}

func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	prev_val, existed := kv.mem[string(key)]
	kv.mem[string(key)] = val
	updated = !existed || !bytes.Equal(prev_val,val)
	return
}

func (kv *KV) Del(key []byte) (deleted bool, err error) {
	_, deleted = kv.mem[string(key)]
	delete(kv.mem, string(key))
	return

}