package kvdb

import (
	"encoding/binary"
	"io"
)

type Entry struct {
	key []byte
	val []byte
}

const (
	lengthSize = 8  
	entryHeaderSize = 2 * lengthSize
)

/* 
All integer fields are encoded using Little Endian.

Serialization Format (binary encoded)
| key size | val size | key data | val data |
| 8 bytes  | 8 bytes  |   ...    |   ...    |
*/
func (ent *Entry) Encode() []byte {
	size := entryHeaderSize + len(ent.key) + len(ent.val)
	data := make([]byte, size)
	
	binary.LittleEndian.PutUint64(data[:lengthSize],uint64(len(ent.key)))
	binary.LittleEndian.PutUint64(data[lengthSize:entryHeaderSize],uint64(len(ent.val)))
	copy(data[entryHeaderSize:],ent.key)
	copy(data[entryHeaderSize+len(ent.key):],ent.val)
	
	return data
}

func (ent *Entry) Decode(r io.Reader) error {
	
	size := make([]byte,entryHeaderSize)

	if _, err := io.ReadFull(r,size); err != nil { return err }
	
	keyLength := binary.LittleEndian.Uint64(size[:lengthSize])
	
	valueLength := binary.LittleEndian.Uint64(size[lengthSize:])

	data := make([]byte,keyLength+valueLength)

	if _, err := io.ReadFull(r,data); err != nil { return err }

	ent.key = data[:keyLength]
	ent.val = data[keyLength:]

	return nil
}


