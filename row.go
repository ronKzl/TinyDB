package kvdb

import "errors"

type Schema struct {
	Table string
	Cols  []Column
	PKey  []int // primary keys are the indexes to the Cols
}

type Column struct {
	Name string
	Type CellType
}

type Row []Cell

func (schema *Schema) NewRow() Row {
	return make(Row, len(schema.Cols))
}

func (row Row) EncodeKey(schema *Schema) (key []byte){
	key = append(key, []byte(schema.Table)...)
	key = append(key, 0x00)
	check(len(row) == len(schema.Cols)) 
	for _, primaryKey := range(schema.PKey) {
		//check that the value in the cell conforms to the collumn type
		check(schema.Cols[primaryKey].Type == row[primaryKey].Type)
		key = row[primaryKey].EncodeKey(key)
	}
	return key
}

func (row Row) EncodeVal(schema *Schema) (val []byte){ 
	
	check(len(schema.Cols) == len(row))

	set := map[int]int{}
	for _, key := range(schema.PKey){
		set[key] = 1
	}
	
	for index, value := range(row) {
		_, ok := set[index]
		if !ok {
			check(value.Type == schema.Cols[index].Type)
			val = value.EncodeVal(val)
		}
	}
	return val 
 }

func (row Row) DecodeKey(schema *Schema, key []byte) (err error){ 
	check(len(row) == len(schema.Cols))

	if len(key) < len(schema.Table) + 1 {
		return errors.New("Bad Key")
	}
	
	if string(key[:len((schema.Table))+1]) != schema.Table+"\x00"{
		return errors.New("Bad Key")
	}

	key = key[len((schema.Table))+1:]

	for _, PKI := range(schema.PKey){
		cell := Cell{Type: schema.Cols[PKI].Type}
		leftOverStream, err := cell.DecodeKey(key) 
		if err != nil {return err}
		key = leftOverStream
		row[PKI] = cell
	}

	if len(key) > 0 {
		return errors.New("Trailing garbage detected")
	}

	return nil 
}

func (row Row) DecodeVal(schema *Schema, val []byte) (err error){ 
	
	check(len(row) == len(schema.Cols))
	set := map[int]int{}

	for _, key := range(schema.PKey){
		set[key] = 1
	}

	for index, col := range(schema.Cols){
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


