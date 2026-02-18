package kvdb

import "errors"

// Schema defines the structure of a database table, composed of the table name,
// a slice of columns, and a slice of primary key indices.
type Schema struct {
	Table string
	Cols  []Column
	PKey  []int // primary keys are the indexes to the Cols
}

// Column defines a single field within a table schema, including its name and data type.
type Column struct {
	Name string
	Type CellType
}

// Row represents a single record in the database, consisting of a slice of Cells.
type Row []Cell

// ErrOutOfRange is returned when a cursor or key moves beyond the expected table boundaries.
var ErrOutOfRange = errors.New("out of range")

// NewRow allocates and returns a new Row sized according to the number of columns in the Schema.
func (schema *Schema) NewRow() Row {
	return make(Row, len(schema.Cols))
}

// EncodeKey serializes the primary key columns of the Row into an order-preserving byte slice.
// It includes the table name and a null terminator as a prefix.
//
// Format: table_name 0x00 [KEY_TYPE Encoded Primary Keys...] 0x00
func (row Row) EncodeKey(schema *Schema) (key []byte) {
	key = append(key, []byte(schema.Table)...)
	key = append(key, 0x00)
	check(len(row) == len(schema.Cols))
	for _, primaryKey := range schema.PKey {
		//check that the value in the cell conforms to the column type
		check(schema.Cols[primaryKey].Type == row[primaryKey].Type)
		key = append(key, byte(row[primaryKey].Type)) // avoids 0xff collisions
		key = row[primaryKey].EncodeKey(key)
	}
	return append(key, 0x00) // avoids negative infinity
}

// EncodeKeyPrefix serializes a partial primary key into an order-preserving byte slice.
// It pads the resulting key with positive infinity (0xff) if positive is true, 
// or negative infinity (implicit 0x00) if false, to establish scan boundaries.
//
// Format: table_name 0x00 [KEY_TYPE Encoded Primary Keys...] (0xff | 0x00)
func EncodeKeyPrefix(schema *Schema, prefix []Cell, positive bool) []byte {
	key := append([]byte(schema.Table), 0x00)

	for idx, cell := range prefix {
		check(cell.Type == schema.Cols[schema.PKey[idx]].Type)
		key = append(key, byte(cell.Type)) // avoids 0xff collisions
		key = cell.EncodeKey(key)
	}
	if positive {
		key = append(key, 0xff) // +infinity
	} // otherwise it will have a 0x00 which is -infinity
	return key

}

// EncodeVal serializes the non-primary key columns of the Row into a byte slice.
//
// Format: [Encoded Values...]
func (row Row) EncodeVal(schema *Schema) (val []byte) {

	check(len(schema.Cols) == len(row))

	set := map[int]int{}
	for _, key := range schema.PKey {
		set[key] = 1
	}

	for index, value := range row {
		_, ok := set[index]
		if !ok {
			check(value.Type == schema.Cols[index].Type)
			val = value.EncodeVal(val)
		}
	}
	return val
}

// DecodeKey deserializes and parses an order-preserving byte stream to populate the primary key cells of the Row.
// It returns ErrOutOfRange if the table name prefix does not match the provided schema.
func (row Row) DecodeKey(schema *Schema, key []byte) (err error) {
	check(len(row) == len(schema.Cols))

	if len(key) < len(schema.Table)+1 {
		return ErrOutOfRange
	}

	if string(key[:len((schema.Table))+1]) != schema.Table+"\x00" {
		return ErrOutOfRange
	}

	key = key[len((schema.Table))+1:]

	for _, PKI := range schema.PKey {
		cell := Cell{Type: schema.Cols[PKI].Type}
		key = key[1:] // for each key get rid of the prepending byte type
		leftOverStream, err := cell.DecodeKey(key)
		if err != nil {
			return err
		}
		key = leftOverStream
		row[PKI] = cell
	}

	if !(len(key) == 1 && key[0] == 0x00) {
		return errors.New("Trailing garbage detected - key was not successfully closed.")
	}

	return nil
}

// DecodeVal deserializes and parses a byte stream to populate the non-primary key cells of the Row.
func (row Row) DecodeVal(schema *Schema, val []byte) (err error) {

	check(len(row) == len(schema.Cols))
	set := map[int]int{}

	for _, key := range schema.PKey {
		set[key] = 1
	}

	for index, col := range schema.Cols {
		_, PK := set[index]
		if !PK {
			cell := Cell{Type: col.Type}
			leftOverStream, err := cell.DecodeVal(val)
			if err != nil {
				return err
			}
			val = leftOverStream
			row[index] = cell
		}
	}

	if len(val) > 0 {
		return errors.New("Trailing garbage detected")
	}

	return nil
}
