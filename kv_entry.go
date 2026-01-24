package kvdb

import (
	"encoding/binary"
	"hash/crc64"
	"io"
	"errors"
)

type Entry struct {
	key []byte
	val []byte
	deleted bool
}

const (
	lengthSize = 8  
	entryHeaderSize = 4 * lengthSize
)
var ErrBadSum = errors.New("bad checksum")
var tab *crc64.Table = crc64.MakeTable(crc64.ISO)

/* 
All integer fields are encoded using Little Endian.

Serialization Format (binary encoded)
| key size | val size | checksum | deleted  | key data | val data |
| 8 bytes  | 8 bytes  | 8 bytes  | 8 bytes  | ...    |   ...    |
*/
func (ent *Entry) Encode() []byte {
	size := entryHeaderSize + len(ent.key) + len(ent.val)
	data := make([]byte, size)
	var isDeleted uint64
	if ent.deleted {
		isDeleted = 1
	}
	
	var hash uint64 = 0
	hash = crc64.Checksum(ent.key, tab)
	hash = crc64.Update(hash,tab,ent.val)
	binary.LittleEndian.PutUint64(data[:lengthSize],uint64(len(ent.key)))
	binary.LittleEndian.PutUint64(data[lengthSize:2*lengthSize],uint64(len(ent.val)))
	binary.LittleEndian.PutUint64(data[2*lengthSize:3*lengthSize],hash)
	binary.LittleEndian.PutUint64(data[3*lengthSize:entryHeaderSize],isDeleted)
	copy(data[entryHeaderSize:],ent.key)
	copy(data[entryHeaderSize+len(ent.key):],ent.val)
	
	return data
}

func (ent *Entry) Decode(r io.Reader) error {
	
	size := make([]byte,entryHeaderSize)

	if _, err := io.ReadFull(r,size); err != nil { return err }
	
	keyLength := binary.LittleEndian.Uint64(size[:lengthSize])
	
	valueLength := binary.LittleEndian.Uint64(size[lengthSize:2*lengthSize])

	logHash := binary.LittleEndian.Uint64(size[2*lengthSize:3*lengthSize])
	
	deletedInt := binary.LittleEndian.Uint64(size[3*lengthSize:])

	data := make([]byte,keyLength+valueLength)

	if _, err := io.ReadFull(r,data); err != nil { return err }

	ent.key = data[:keyLength]
	ent.deleted = deletedInt == 1
	if !ent.deleted {
		ent.val = data[keyLength:]
	}

	// after entry is done need to compare the hash of it
	var curHash uint64 = 0
	curHash = crc64.Checksum(ent.key, tab)
	curHash = crc64.Update(curHash,tab,ent.val)
	if logHash != curHash {
		return ErrBadSum
	}

	return nil
}


