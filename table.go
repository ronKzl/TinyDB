package kvdb

import (
	"encoding/json"
	"errors"
	"strings"
)

type DB struct {
	KV     KV
	tables map[string]Schema
}

type SQLResult struct {
	Updated int
	Header  []string
	Values  []Row
}

func (db *DB) Open() error {
	db.tables = map[string]Schema{}
	return db.KV.Open()
}
func (db *DB) Close() error { return db.KV.Close() }

func (db *DB) Select(schema *Schema, row Row) (ok bool, err error) {
	key := row.EncodeKey(schema)
	value, ok, err := db.KV.Get(key)

	if !ok || err != nil {
		return ok, err
	}
	if err = row.DecodeVal(schema, value); err != nil {
		return false, err
	}

	return true, nil

}

func (db *DB) Insert(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)
	return db.KV.SetEx(key, val, ModeInsert)
}

func (db *DB) Upsert(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)
	return db.KV.SetEx(key, val, ModeUpsert)
}

func (db *DB) Update(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)
	return db.KV.SetEx(key, val, ModeUpdate)
}

func (db *DB) Delete(schema *Schema, row Row) (deleted bool, err error) {
	key := row.EncodeKey(schema)
	return db.KV.Del(key)
}

func (db *DB) ExecStmt(stmt interface{}) (r SQLResult, err error) {
	switch ptr := stmt.(type) {
	case *StmtCreatTable:
		err = db.execCreateTable(ptr)
	case *StmtSelect:
		r.Header = ptr.cols
		r.Values, err = db.execSelect(ptr)
	case *StmtInsert:
		r.Updated, err = db.execInsert(ptr)
	case *StmtUpdate:
		r.Updated, err = db.execUpdate(ptr)
	case *StmtDelete:
		r.Updated, err = db.execDelete(ptr)
	default:
		panic("unreachable")
	}
	return
}

func lookupColumns(cols []Column, pkeys []string) ([]int, error) {
	PKeyIndex := make([]int,0)
	
	for _, keyName := range(pkeys) {
		found := false
		for index, colName := range(cols){
			if strings.EqualFold(keyName,colName.Name) {
				PKeyIndex = append(PKeyIndex, index)
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New("Primary key name: " + keyName + " was not found in cols")
		}
	}

	return PKeyIndex, nil
}

func makePKey(schema *Schema, pkeys []NamedCell) (Row, error) {

	if len(schema.PKey) != len(pkeys) {
		return nil, errors.New("Not a primary key")
	}

	row := schema.NewRow()

	for _, pkey := range(schema.PKey){
		found := false
		for _, cell := range(pkeys) {
			if strings.EqualFold(schema.Cols[pkey].Name, cell.column) && schema.Cols[pkey].Type == cell.value.Type {
				row[pkey] = cell.value
				found = true
			}
		}
		if !found {
			return nil, errors.New("Not a primary key")
		}
	}

	return row, nil
} 

func subsetRow(row Row, indices []int) (updated Row) {
	for _, PKeyIndex := range(indices) {
		updated = append(updated, row[PKeyIndex])
	}

	return updated 
} 

func (db *DB) execCreateTable(stmt *StmtCreatTable) (err error) {
	if strings.EqualFold(stmt.table, ""){
		return errors.New("Table name must not be empty")
	}
		
	if _, err := db.GetSchema(stmt.table); err == nil {
		return errors.New("Table under the name: " + stmt.table + " already exists!")
	}

	schema := Schema{Table:stmt.table,Cols: stmt.cols}

	if schema.PKey, err = lookupColumns(stmt.cols, stmt.pkey); err != nil {
		return err
	}

	info, err := json.Marshal(schema)
	check(err == nil)
	updated, err := db.KV.Set([]byte("@schema_" + stmt.table), info)

	if !updated || err != nil {
		return err
	}

	db.tables[schema.Table] = schema

	return nil 
}

func (db *DB) GetSchema(table string) (Schema, error) {
	schema, ok := db.tables[table]
	if !ok {
		val, ok, err := db.KV.Get([]byte("@schema_" + table))
		if err == nil && ok {
			err = json.Unmarshal(val, &schema)
		}
		if err != nil {
			return Schema{}, err
		}
		if !ok {
			return Schema{}, errors.New("table is not found")
		}
		db.tables[table] = schema
	}
	return schema, nil
}

func (db *DB) execSelect(stmt *StmtSelect) ([]Row, error){
	schema, err := db.GetSchema(stmt.table)
	if err != nil {
		return nil, err
	}

	indices, err := lookupColumns(schema.Cols, stmt.cols)
	if err != nil {
		return nil, err
	}

	row, err := makePKey(&schema, stmt.keys)
	if err != nil {
		return nil, err
	}
	
	if ok, err := db.Select(&schema,row); err != nil || !ok {
		return nil, err
	}
	
	row = subsetRow(row,indices)

	return []Row{row}, nil
}

func (db *DB) execInsert(stmt *StmtInsert) (count int, err error) {
	
	schema, err := db.GetSchema(stmt.table)
	
	if err != nil {
		return 0, err
	}

	if len(stmt.value) != len(schema.Cols) {
		return count, errors.New("schema mismatch")
	}

	for i := range(schema.Cols) {
		if schema.Cols[i].Type != stmt.value[i].Type {
			return count, errors.New("schema mismatch")
		}
	}

	updated, err := db.Insert(&schema,stmt.value)
	
	if err != nil {
		return 0, err
	}
	if updated {
		count += 1
	}
	
	return count, nil
}

func (db *DB) execUpdate(stmt *StmtUpdate) (count int, err error){
	
	// schema ,err := db.GetSchema(stmt.table)

	// if err != nil {
	// 	return count, err
	// }
	
	// TODO: not allow updating PKs
	

	//db.Update(&schema,stmt.value)
	
	
	return count, nil
}

func (db *DB) execDelete(stmt *StmtDelete) (count int, err error){
	schema ,err := db.GetSchema(stmt.table)

	if err != nil {
		return count, err
	}

	row, err := makePKey(&schema, stmt.keys)

	if err != nil {
		return count, err 
	}

	updated, err := db.Delete(&schema,row)
	
	if err != nil {
		return count, err
	}

	if updated {
		count = count + 1
	}
	
	return count, nil

}
