package kvdb

import (
	"encoding/binary"
	"errors"
	"slices"
)

type CellType uint8

// CellType represents the supported data types for database cells.
const (
	TypeI64 CellType = 1 // 64-bit signed integer
	TypeStr CellType = 2 // Variable-length string/byte slice
)

// nullTerminator marks the end of an encoded string key.
const nullTerminator = 0x00

// escapeChar is used to escape null bytes within string keys to maintain sort order.
const escapeChar = 0x01

// Cell represents a single data unit in a database row. It stores both the
// type metadata and the actual value (int64 or byte slice).
type Cell struct {
	Type CellType
	I64  int64
	Str  []byte
}

// encodeStrKey serializes a byte slice into an order preserving format.
// It escapes null bytes and the escape character to ensure the resulting
// slice can be safely compared lexicographically.
func encodeStrKey(toAppend []byte, input []byte) []byte {
	for _, chr := range input {
		if chr == nullTerminator || chr == escapeChar {
			toAppend = append(toAppend, escapeChar, chr+1)
		} else {
			toAppend = append(toAppend, chr)
		}
	}
	return append(toAppend, nullTerminator)
}

// decodeStrKey deserializes an order preserving byte slice back into its original
// form. It returns the decoded string and the remaining bytes in the stream.
func decodeStrKey(data []byte) (out []byte, rest []byte, err error) {
	escape := false
	for idx, chr := range data {
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

// EncodeKey serializes the Cell into a byte slice that preserves the natural
// sort order of the data. Integers are encoded in Big-Endian with the sign
// bit flipped to ensure negative numbers sort before positive ones.
func (cell *Cell) EncodeKey(toAppend []byte) []byte {
	switch cell.Type {
	case TypeI64:
		// Flip the MSB (sign bit) to make signed integers sortable as unsigned bytes.
		return binary.BigEndian.AppendUint64(toAppend, uint64(cell.I64)^(1<<63))
	case TypeStr:
		return encodeStrKey(toAppend, cell.Str)
	default:
		panic("Can't be encoded")
	}
}

// DecodeKey parses an order preserving byte slice and populates the Cell.
// It returns the remaining unparsed bytes in the stream.
func (cell *Cell) DecodeKey(data []byte) (rest []byte, err error) {
	switch cell.Type {
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

// EncodeVal serializes the Cell into a compact binary format for storage.
// It uses Little-Endian for integers and length-prefixed encoding for strings.
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

// DecodeVal parses a serialized value byte slice and populates the Cell.
// It returns the remaining unparsed bytes in the stream.
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
