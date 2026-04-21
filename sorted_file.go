package kvdb

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	nkeys    int
}

// Close releases the underlying file descriptor.
// It is the caller's responsibility to ensure no active reads are occurring
// when Close is invoked, as it will cause subsequent reads to panic or fail.
func (file *SortedFile) Close() error {
	return file.fp.Close()
}

// CreateFromSorted flushes an in-memory SortedKV to an immutable on-disk SSTable.
func (file *SortedFile) CreateFromSorted(kv SortedKV) (err error) {
	// ADD THIS LINE: Track the number of keys in the struct state
	file.nkeys = kv.Size()

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

func (file *SortedFile) Size() int { return file.nkeys }
func (file *SortedFile) Iter() (SortedKVIter, error) {
	iter := &SortedFileIter{file: file, pos: 0}
	if err := iter.loadCurrent(); err != nil {
		return nil, err
	}
	return iter, nil
}

// Open initializes the SortedFile by reading the 8-byte header to determine
// the total number of keys. This allows bounds checking during iteration.
func (file *SortedFile) Open() (err error) {
	file.fp, err = os.OpenFile(file.FileName, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	var buf [offsetSize]byte
	if _, err = file.fp.ReadAt(buf[:], 0); err != nil {
		return err
	}
	file.nkeys = int(binary.LittleEndian.Uint64(buf[:]))
	return nil
}

// index retrieves the key and value at the specified logical position within the SSTable.
// It performs 3 unbuffered random reads: 1 for the offset, 1 for the lengths, and 1 for the payload.
// Performance Note: This is an unoptimized read. Production DBs typically bypass the page cache
// using direct IO or memory mapping (mmap) to avoid heavy syscall overhead.
func (file *SortedFile) index(pos int) (key []byte, val []byte, err error) {
	if pos < 0 || pos >= file.nkeys {
		return nil, nil, errors.New("index out of bounds")
	}

	var buf [offsetSize]byte
	// 1. Read the offset for the KV pair from the Offset Index block.
	// The Offset Index starts immediately after the 8-byte header.
	if _, err = file.fp.ReadAt(buf[:], int64(8+8*pos)); err != nil {
		return nil, nil, err
	}
	offset := int64(binary.LittleEndian.Uint64(buf[:]))

	// Sanity check: Offset must point to the Data Blocks area, which strictly
	// follows the 8-byte header and the N*8-byte Offset Index.
	if offset < int64(8+8*file.nkeys) {
		return nil, nil, errors.New("corrupted file: invalid KV offset")
	}

	// 2. Read the lengths of the Key and Value.
	if _, err = file.fp.ReadAt(buf[:], offset); err != nil { // buf is 8 bytes, perfect for 2x uint32
		return nil, nil, err
	}
	klen := binary.LittleEndian.Uint32(buf[0:4])
	vlen := binary.LittleEndian.Uint32(buf[4:8])

	// 3. Read the actual Key and Value payloads.
	data := make([]byte, klen+vlen)
	if _, err = file.fp.ReadAt(data, offset+int64(KVLength+KVLength)); err != nil {
		return nil, nil, err
	}

	return data[:klen], data[klen:], nil
}

// Seek finds the first KV pair where Key >= target.
// It utilizes O(log N) binary search over the on-disk offset index.
// It returns a generic SortedKVIter interface to maintain loose coupling across the engine.
func (file *SortedFile) Seek(key []byte) (SortedKVIter, error) {
	low, high := 0, file.nkeys
	for low < high {
		mid := low + (high-low)/2
		mKey, _, err := file.index(mid)
		if err != nil {
			return nil, err
		}
		if bytes.Compare(mKey, key) < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}

	iter := &SortedFileIter{file: file, pos: low}
	if err := iter.loadCurrent(); err != nil {
		return nil, err
	}
	return iter, nil
}

// SortedFileIter implements SortedKVIter for on-disk SSTables.
type SortedFileIter struct {
	file *SortedFile
	pos  int
	key  []byte
	val  []byte
}

// Valid checks if the iterator is within the bounds of the file's keys.
func (iter *SortedFileIter) Valid() bool {
	return 0 <= iter.pos && iter.pos < iter.file.nkeys
}

func (iter *SortedFileIter) Key() []byte { return iter.key }
func (iter *SortedFileIter) Val() []byte { return iter.val }

// loadCurrent updates the iterator's state with the KV data at the current logical position.
func (iter *SortedFileIter) loadCurrent() error {
	if !iter.Valid() {
		iter.key, iter.val = nil, nil
		return nil
	}
	key, val, err := iter.file.index(iter.pos)
	if err != nil {
		return err
	}
	iter.key, iter.val = key, val
	return nil
}

// Next advances the iterator by one position.
func (iter *SortedFileIter) Next() error {
	iter.pos++
	return iter.loadCurrent()
}

// Prev reverses the iterator by one position.
func (iter *SortedFileIter) Prev() error {
	iter.pos--
	return iter.loadCurrent()
}
