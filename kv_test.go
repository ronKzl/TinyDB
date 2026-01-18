package kvdb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKVBasic(t *testing.T) {
	kv := KV{} // create instance
	err := kv.Open()
	assert.Nil(t, err)
	defer kv.Close() // wait until the entire test is done to close

	// put k1 == v1
	updated, err := kv.Set([]byte("k1"), []byte("v1"))
	assert.True(t, updated && err == nil)

	// get k1 -> success
	val, ok, err := kv.Get([]byte("k1"))
	assert.True(t, string(val) == "v1" && ok && err == nil)

	// get xxx -> not found
	_, ok, err = kv.Get([]byte("xxx"))
	assert.True(t, !ok && err == nil)

	// del xxx -> not found
	updated, err = kv.Del([]byte("xxx"))
	assert.True(t, !updated && err == nil)

	// del k1 -> success
	updated, err = kv.Del([]byte("k1"))
	assert.True(t, updated && err == nil)

	// get xxx -> not found
	_, ok, err = kv.Get([]byte("xxx"))
	assert.True(t, !ok && err == nil)

	// get k1 -> not found
	_, ok, err = kv.Get([]byte("k1"))
	assert.True(t, !ok && err == nil)
}

func TestEntryEncode(t *testing.T) {
	ent := Entry{key: []byte("k1"), val: []byte("xxx")}
	data := []byte{2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 'k', '1', 'x', 'x', 'x'}

	assert.Equal(t, data, ent.Encode())

	decoded := Entry{}
	err := decoded.Decode(bytes.NewBuffer(data))
	assert.Nil(t, err)
	assert.Equal(t, ent, decoded)
}
