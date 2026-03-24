package kvdb

import (
	"encoding/binary"
	"os"
)

const (
	KVLength   int = 4
	offsetSize int = 8
)

// SortedKV defines the contract for an in-memory collection of sorted key-value pairs.
// Implementations must guarantee that the data remains immutable during iteration
// to prevent torn reads or index out-of-bounds errors while flushing to disk.
type SortedKV interface {
	Size() int
	Iter() (SortedKVIter, error)
}

// SortedKVIter defines the contract for traversing a SortedKV.
// Callers must check Valid() before accessing Key() or Val().
// The iterator is not guaranteed to be thread-safe.
type SortedKVIter interface {
	Valid() bool
	Key() []byte
	Val() []byte
	Next() error
	Prev() error
}

// SortedFile represents an immutable, on-disk Sorted String Table (SSTable).
// Once created, the underlying file is strictly read-only.
type SortedFile struct {
	FileName string
	fp       *os.File
}

// Close releases the underlying file descriptor.
// It is the caller's responsibility to ensure no active reads are occurring
// when Close is invoked, as it will cause subsequent reads to panic or fail.
func (file *SortedFile) Close() error {
	return file.fp.Close()
}

// CreateFromSorted flushes an in-memory SortedKV to an immutable on-disk SSTable.
// It guarantees atomicity and durability: if the write fails midway, the file
// descriptor is closed, and the caller is expected to handle cleanup.
func (file *SortedFile) CreateFromSorted(kv SortedKV) (err error) {
	if file.fp, err = createFileSync(file.FileName); err != nil {
		return err
	}
	if err = file.writeSortedFile(kv); err != nil {
		_ = file.Close()
	}
	return err
}

// writeSortedFile serializes the SortedKV into the specific SSTable binary format.
// Format Layout:
//  1. Header: 8 bytes (uint64) total key count.
//  2. Offset Index: N * 8 bytes (uint64 array). Points to the start of each KV pair.
//  3. Data Blocks: [4 byte KeyLen][4 byte ValLen][Key Bytes][Val Bytes] repeated.
//
// Performance Note: This implementation currently uses multiple unbuffered WriteAt calls.
// tech debt to refactor to minimize costly syscall context switches.
func (file *SortedFile) writeSortedFile(kv SortedKV) (err error) {
	var buf [KVLength + KVLength]byte
	// Write Header: Total number of keys.
	binary.LittleEndian.PutUint64(buf[:8], uint64(kv.Size()))
	if _, err = file.fp.WriteAt(buf[:8], 0); err != nil {
		return err
	}

	nkeys := 0
	// The data block starts immediately after the 8-byte header and the offset array.
	offset := offsetSize + offsetSize*kv.Size()
	iter, err := kv.Iter()
	for ; err == nil && iter.Valid(); err = iter.Next() {
		key, val := iter.Key(), iter.Val()
		// Write Offset Index: Record exactly where this KV pair will live on disk.
		// This enables O(log N) binary search directly on the file later.
		binary.LittleEndian.PutUint64(buf[:8], uint64(offset))
		if _, err = file.fp.WriteAt(buf[:8], int64(8+8*nkeys)); err != nil {
			return err
		}

		// Write Data Block: Key and Value lengths.
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
		binary.LittleEndian.PutUint32(buf[4:8], uint32(len(val)))
		if _, err = file.fp.WriteAt(buf[:4+4], int64(offset)); err != nil {
			return err
		}
		// Write Data Block: The actual Key bytes.
		offset += KVLength + KVLength
		if _, err = file.fp.WriteAt(key, int64(offset)); err != nil {
			return err
		}
		// Write Data Block: The actual Value bytes.
		offset += len(key)
		if _, err = file.fp.WriteAt(val, int64(offset)); err != nil {
			return err
		}
		// Advance offset for the next iteration.
		offset += len(val)
		nkeys++
	}
	if err != nil {
		return err
	}
	// Ensure the iterator yielded the exact number of keys expected.
	// If this fails, the in-memory structure mutated during the flush (most likely a race condition).
	check(nkeys == kv.Size())

	// Durability Guarantee: Force the OS page cache to flush to physical media.
	return file.fp.Sync()
}
