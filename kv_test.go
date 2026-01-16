package kvdb

import (
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
