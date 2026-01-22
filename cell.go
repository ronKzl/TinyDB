package kvdb

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
	case TypeStr:
	}
	return nil
}

func (cell *Cell) Decode(data []byte) (rest []byte, err error){


}

