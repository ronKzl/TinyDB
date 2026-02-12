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

const nullTerminator = 0x00
const escapeChar = 0x01

type Cell struct {
	Type CellType
	I64  int64
	Str  []byte
}


func encodeStrKey(toAppend []byte, input []byte) []byte {
	for _, chr := range(input) {
		if chr == nullTerminator || chr == escapeChar {
			toAppend = append(toAppend, escapeChar, chr+1)
		}else {
			toAppend = append(toAppend, chr)
		}
	}
	return append(toAppend, nullTerminator)
}

func decodeStrKey(data []byte) (out []byte, rest []byte, err error){
	escape := false 
	for idx, chr := range(data) {
		if !escape {
			if chr == nullTerminator {
				return out, data[idx+1:], nil 
			} 
			if chr == escapeChar {
				escape = true
				continue
			}
			out = append(out, chr)
		} else {
			out = append(out, chr-1)
			escape = false
		}
	}

	return nil, data, errors.New("data not null terminated")
}


func (cell *Cell) EncodeKey(toAppend []byte) []byte {
	switch cell.Type{
	case TypeI64:
		return binary.BigEndian.AppendUint64(toAppend, uint64(cell.I64)^(1 << 63))
	case TypeStr:
		return encodeStrKey(toAppend,cell.Str)
	default:
		panic("Can't be encoded")
	}
}

func (cell *Cell) DecodeKey(data []byte) (rest []byte, err error) {
	switch cell.Type{
	case TypeI64:
		if len(data) < lengthSize {
			return data, errors.New("Expected more data")
		}
		cell.I64 = int64(binary.BigEndian.Uint64(data[:lengthSize]) ^ (1 << 63))
		return data[lengthSize:], nil
	case TypeStr:
		cell.Str, rest, err = decodeStrKey(data)
		return rest, err
	default:
		panic("Can't be reached")
	}
}

func (cell *Cell) EncodeVal(toAppend []byte) []byte {
	switch cell.Type {
	case TypeI64:
		return binary.LittleEndian.AppendUint64(toAppend, uint64(cell.I64))
	case TypeStr:
		toAppend = binary.LittleEndian.AppendUint64(toAppend, uint64(len(cell.Str)))
		return append(toAppend, cell.Str...)
	default:
		panic("Can't be encoded")
	}
}

func (cell *Cell) DecodeVal(data []byte) (rest []byte, err error) {
	switch cell.Type {
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
		cell.Str = slices.Clone(data[lengthSize : size+lengthSize])
		return data[lengthSize+size:], nil
	default:
		panic("Can't be decoded")
	}
}
