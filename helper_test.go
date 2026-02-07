package kvdb

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestBinarySearch(t *testing.T){

	var arr = []int{2,4,6,8}

	compareFunc := func(a int, b int) int {return a - b}

	idx, ok := BinarySearchFunc(arr,4,compareFunc)
	assert.Equal(t,idx,1)
	assert.Equal(t,ok,true)

	idx, ok = BinarySearchFunc(arr,5,compareFunc)
	assert.Equal(t,idx,2)
	assert.Equal(t,ok,false)

	arr = []int{2,4,5,6,8}
	idx, ok = BinarySearchFunc(arr,5,compareFunc)
	assert.Equal(t,idx,2)
	assert.Equal(t,ok,true)

	idx, ok = BinarySearchFunc(arr,1,compareFunc)
	assert.Equal(t,idx,0)
	assert.Equal(t,ok,false)

	idx, ok = BinarySearchFunc(arr,9,compareFunc)
	assert.Equal(t,idx,5)
	assert.Equal(t,ok,false)
}