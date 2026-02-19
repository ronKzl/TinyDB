package kvdb

import (
	"encoding/json"
	"errors"
	"strings"
)

// DB contains the KV engine and manages the schema for all tables.
// It matches the in-memory schema to the persistent storage.
type DB struct {
	KV     KV
	tables map[string]Schema
}

// SQLResult holds the outcome of an executed query, including
// the number of rows updated and any returned data.
type SQLResult struct {
	Updated int
	Header  []string
	Values  []Row
}

// RowIterator wraps a RangedKVIter to provide a relational view of the data.
// It handles decoding raw bytes into structured Rows and tracking table boundaries.
type RowIterator struct {
	schema *Schema
	iter   *RangedKVIter
	valid  bool
	row    Row
}

// RangeReq defines the boundaries and comparison operators for a table scan
type RangeReq struct {
	StartCmp ExprOp // Comparison for the start boundary (e.g., OP_GE)
	StopCmp  ExprOp // Comparison for the stop boundary (e.g., OP_LE)
	Start    []Cell // Start primary key or prefix
	Stop     []Cell // Stop primary key or prefix
}

// Open initializes the DB struct and calls Open on KV database
func (db *DB) Open() error {
	db.tables = map[string]Schema{}
	return db.KV.Open()
}

// Close closes the KV database
func (db *DB) Close() error { return db.KV.Close() }

// Select populates the provided row with data from the database.
// It returns true if the row was found and successfully decoded, or false if not found.
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

// Insert inserts a new Row into the database given the schema and Row.
func (db *DB) Insert(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)
	return db.KV.SetEx(key, val, ModeInsert)
}

// Upsert inserts or updates an existing key-value pair in the database given the schema and the updated Row.
func (db *DB) Upsert(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)
	return db.KV.SetEx(key, val, ModeUpsert)
}

// Update modifies the value for an existing key in the database given the schema and Row.
func (db *DB) Update(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)
	return db.KV.SetEx(key, val, ModeUpdate)
}

// Delete generates the encoded key from the provided Schema and Row, and deletes the corresponding entry.
func (db *DB) Delete(schema *Schema, row Row) (deleted bool, err error) {
	key := row.EncodeKey(schema)
	return db.KV.Del(key)
}

// ExecStmt evaluates the provided statement interface and executes the corresponding database operation.
// It returns an SQLResult object which holds the outcome of the executed query.
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

// lookupColumns is a helper that returns an indices slice of the primary keys in the Column slice,
// given a Column slice and a primary key name slice.
func lookupColumns(cols []Column, pkeys []string) ([]int, error) {
	PKeyIndex := make([]int, 0)

	for _, keyName := range pkeys {
		found := false
		for index, colName := range cols {
			if strings.EqualFold(keyName, colName.Name) {
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

// makePKey constructs and returns the row of the primary key given a database Schema and a NamedCell slice.
func makePKey(schema *Schema, pkeys []NamedCell) (Row, error) {

	if len(schema.PKey) != len(pkeys) {
		return nil, errors.New("Not a primary key")
	}

	row := schema.NewRow()

	for _, pkey := range schema.PKey {
		found := false
		for _, cell := range pkeys {
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

// subsetRow is a helper that returns a subset of the provided Row consisting only of the given indices.
func subsetRow(row Row, indices []int) (updated Row) {
	for _, PKeyIndex := range indices {
		updated = append(updated, row[PKeyIndex])
	}

	return updated
}

// execCreateTable creates a new table given a StmtCreatTable.
func (db *DB) execCreateTable(stmt *StmtCreatTable) (err error) {
	if strings.EqualFold(stmt.table, "") {
		return errors.New("Table name must not be empty")
	}

	if _, err := db.GetSchema(stmt.table); err == nil {
		return errors.New("Table under the name: " + stmt.table + " already exists!")
	}

	schema := Schema{Table: stmt.table, Cols: stmt.cols}

	if schema.PKey, err = lookupColumns(stmt.cols, stmt.pkey); err != nil {
		return err
	}

	info, err := json.Marshal(schema)
	check(err == nil)
	updated, err := db.KV.Set([]byte("@schema_"+stmt.table), info)

	if !updated || err != nil {
		return err
	}

	db.tables[schema.Table] = schema

	return nil
}

// GetSchema uses the table name to return the schema for that table.
// If the schema is not found in memory, it attempts to fetch it from the key-value store.
// It returns an error if the table cannot be found.
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

// execSelect selects a specific row from the table given a StmtSelect.
// It returns the specific row that matches the given key and any errors that occurred.
func (db *DB) execSelect(stmt *StmtSelect) ([]Row, error) {
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

	if ok, err := db.Select(&schema, row); err != nil || !ok {
		return nil, err
	}

	row = subsetRow(row, indices)

	return []Row{row}, nil
}

// execInsert handles the logic for INSERT statements.
// It validates that the value types match the schema before inserting.
func (db *DB) execInsert(stmt *StmtInsert) (count int, err error) {

	schema, err := db.GetSchema(stmt.table)

	if err != nil {
		return 0, err
	}

	if len(stmt.value) != len(schema.Cols) {
		return 0, errors.New("schema mismatch")
	}

	for i := range schema.Cols {
		if schema.Cols[i].Type != stmt.value[i].Type {
			return 0, errors.New("schema mismatch")
		}
	}

	updated, err := db.Insert(&schema, stmt.value)

	if err != nil {
		return 0, err
	}
	if updated {
		count += 1
	}

	return count, nil
}

// execUpdate attempts to update a row corresponding to the provided statement.
// It returns an error if the user attempts to update a primary key or a non-existent value.
func (db *DB) execUpdate(stmt *StmtUpdate) (count int, err error) {

	schema, err := db.GetSchema(stmt.table)

	if err != nil {
		return 0, err
	}

	row, err := makePKey(&schema, stmt.keys)
	if err != nil {
		return 0, err
	}

	if ok, err := db.Select(&schema, row); err != nil || !ok {
		return 0, err
	}

	for _, updatedValue := range stmt.value {
		found := false
		updatingIndex := 0
		for idx, col := range schema.Cols {
			if strings.EqualFold(updatedValue.column, col.Name) {
				found = true
				updatingIndex = idx
				break
			}
		}
		if !found {
			return 0, errors.New("Attempting to update " + updatedValue.column + " not found in table")
		}

		for _, keyIndex := range schema.PKey {
			if updatingIndex == keyIndex {
				return 0, errors.New("Updating a primary key is not allowed")
			}
		}
		row[updatingIndex] = updatedValue.value
	}

	updated, err := db.Update(&schema, row)

	if err != nil {
		return 0, err
	}
	if updated {
		count += 1
	}
	return count, nil
}

// execDelete handles the logic for DELETE statements.
// It constructs the primary key from the WHERE clause and removes the entry.
func (db *DB) execDelete(stmt *StmtDelete) (count int, err error) {
	schema, err := db.GetSchema(stmt.table)

	if err != nil {
		return 0, err
	}

	row, err := makePKey(&schema, stmt.keys)

	if err != nil {
		return 0, err
	}

	updated, err := db.Delete(&schema, row)

	if err != nil {
		return 0, err
	}

	if updated {
		count += 1
	}

	return count, nil

}

// Valid reports whether the iterator is currently positioned at a valid row belonging to the table.
func (iter *RowIterator) Valid() bool { return iter.valid }

// Row returns the current row. It panics if the iterator is not valid.
// The returned Row is a reference that will be overwritten by the next call to Next.
func (iter *RowIterator) Row() Row { check(iter.valid); return iter.row }

// Next advances the iterator to the next row in the table.
// It updates the Valid status based on whether the next entry belongs to the same table.
func (iter *RowIterator) Next() (err error) {
	if err = iter.iter.Next(); err != nil {
		return err
	}
	iter.valid, err = decodeKVIter(iter.schema, iter.iter, iter.row)
	return err
}

// decodeKVIter is a helper that attempts to decode the current KV pair into the provided row.
// It returns false if the iterator has reached the end of the KV store or a different table prefix.
func decodeKVIter(schema *Schema, iter *RangedKVIter, row Row) (bool, error) {
	if !iter.Valid() {
		return false, nil
	}

	err := row.DecodeKey(schema, iter.Key())
	if err == ErrOutOfRange {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if err := row.DecodeVal(schema, iter.Val()); err != nil {
		return false, err
	}

	return true, nil
}

// Seek positions a RowIterator at the first record greater than or equal to the search key.
// It uses the provided Row to extract primary key columns and performs an ascending scan.
func (db *DB) Seek(schema *Schema, row Row) (*RowIterator, error) {

	start := make([]Cell, len(schema.PKey))

	for idx, PK := range schema.PKey {
		check(row[PK].Type == schema.Cols[PK].Type)
		start[idx] = row[PK]
	}

	// OP_GE is used to find the start and OP_LE as a stop to scan to the table end.
	rangeReq := &RangeReq{StartCmp: OP_GE, StopCmp: OP_LE, Start: start, Stop: nil}
	rowIter, err := db.Range(schema, rangeReq)

	return rowIter, err
}

// suffixPositive returns true if the operator requires positive infinity padding (0xff).
// This is used for <= and > to ensure the boundary includes all possible key suffixes.
func suffixPositive(compareOperation ExprOp) bool {
	if compareOperation == OP_LE || compareOperation == OP_GT {
		return true
	}
	return false
}

// isDescending returns true if the operator implies a backward scan (<= or <).
func isDescending(compareOperation ExprOp) bool {
	if compareOperation == OP_LE || compareOperation == OP_LT {
		return true
	}
	return false
}

// Range returns a RowIterator for the specified boundaries and scan direction.
// It bootstraps the iterator by decoding the first valid KV pair into a Row.
func (db *DB) Range(schema *Schema, req *RangeReq) (*RowIterator, error) {
	start := EncodeKeyPrefix(schema, req.Start, suffixPositive(req.StartCmp))
	stop := EncodeKeyPrefix(schema, req.Stop, suffixPositive(req.StopCmp))
	desc := isDescending(req.StartCmp)
	iter, err := db.KV.Range(start, stop, desc)

	if err != nil {
		return nil, err
	}
	row := schema.NewRow()

	isValid, err := decodeKVIter(schema, iter, row)
	if err != nil {
		return nil, err
	}

	return &RowIterator{schema: schema, iter: iter, valid: isValid, row: row}, nil
}
