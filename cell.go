package kvdb

import (
	"encoding/binary"
	"errors"
	"slices"
)

type CellType uint8

const (
	TypeI64 CellType = 1
	TypeStr CellType = 2
)

type Cell struct {
	Type CellType
	I64  int64
	Str  []byte
}

func (cell *Cell) Encode(toAppend []byte) []byte {
	switch cell.Type {
	case TypeI64:
		toAppend = make([]byte,lengthSize)
		binary.LittleEndian.PutUint64(toAppend,uint64(cell.I64))
		return toAppend
	case TypeStr:
		toAppend = make([]byte, lengthSize+len(cell.Str))
		binary.LittleEndian.PutUint64(toAppend,uint64(len(cell.Str)))
		copy(toAppend[lengthSize:], cell.Str)
		return toAppend
	default:
		panic("Can't be encoded")
	}
}

func (cell *Cell) Decode(data []byte) (rest []byte, err error){
	switch cell.Type{
	case TypeI64:
		if len(data) < lengthSize {
			return data, errors.New("Expected more data")
		}
		cell.I64 = int64(binary.LittleEndian.Uint64(data[:lengthSize]))
		return data[lengthSize:], nil
	case TypeStr:
		if len(data) < lengthSize {
			return data, errors.New("Expected more data")
		}
		size := binary.LittleEndian.Uint64(data[:lengthSize])
		if uint64(len(data)) < lengthSize+size {
			return data, errors.New("Expected more data")
		}
		cell.Str = slices.Clone(data[lengthSize:size+lengthSize])
		return data[lengthSize+size:], nil
	default:
		panic("Can't be decoded")
	}
}

