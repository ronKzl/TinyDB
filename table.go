package kvdb

import (
	"encoding/json"
	"errors"
	"slices"
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
		r.Header = exprsToHeader(ptr.cols)
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

// matchPKey attempts to match the user given query on an expression and
// returns the keys and values from that expression as a row if possible.
func matchPKey(schema *Schema, cond interface{}) (Row, error) {
	if keys, ok := matchAllEq(cond, nil); ok {
		return makePKey(schema, keys)
	}
	return nil, errors.New("unimplemented WHERE")
}

// matchAllEq detects expression like a = 123 AND b = 456 ... and returns
// a single row with the key name and respective value.
func matchAllEq(cond interface{}, out []NamedCell) ([]NamedCell, bool) {
	binop, ok := cond.(*ExprBinOp)
	if ok && binop.op == OP_AND {
		if out, ok = matchAllEq(binop.left, out); !ok {
			return nil, false
		}

		if out, ok = matchAllEq(binop.right, out); !ok {
			return nil, false
		}

		return out, true
	} else if ok && binop.op == OP_EQ {
		left, right := binop.left, binop.right

		// unknown if column name is is on left or right so try left first if not try right
		colName, ok := left.(string)
		if !ok { // not okay try to switcheroo
			left, right = right, left
			colName, ok = left.(string)
		}

		if !ok { // still not right return
			return nil, false
		}

		cell, ok := right.(*Cell)
		if !ok {
			return nil, false
		}
		return append(out, NamedCell{column: colName, value: *cell}), true

	}
	return nil, false
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
func (db *DB) execSelect(stmt *StmtSelect) (output []Row, err error) {

	schema, err := db.GetSchema(stmt.table)
	if err != nil {
		return nil, err
	}

	iter, err := db.execCond(&schema, stmt.cond)
	if err != nil {
		return nil, err
	}

	for ; err == nil && iter.Valid(); err = iter.Next() {
		row := iter.Row()
		computed := make(Row, len(stmt.cols))
		for i, expr := range stmt.cols {
			cell, err := evalExpr(&schema, row, expr)
			if err != nil {
				return nil, err
			}
			computed[i] = *cell
		}
		output = append(output, computed)
	}
	if err != nil {
		return nil, err
	}
	return output, nil
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

// fillNonPKey is a helper that attempts to populate a row in the table with the given updates slice, it will throw
// an error if there is an attempt to update a table primary key or if one of the requested columns to update
// is not found in the table schema.
func fillNonPKey(schema *Schema, updates []NamedCell, out Row) error {
	for _, expr := range updates {
		found := false
		updatingIndex := 0
		for idx, col := range schema.Cols {
			if strings.EqualFold(expr.column, col.Name) && expr.value.Type == col.Type {
				found = true
				updatingIndex = idx
				break
			}
		}
		if !found {
			return errors.New("Attempting to update " + expr.column + " not found in table")
		}
		if slices.Contains(schema.PKey, updatingIndex) {
			return errors.New("cannot update column its a PK")
		}
		out[updatingIndex] = expr.value
	}
	return nil
}

// execUpdate attempts to update a row corresponding to the provided statement.
// It returns an error if the user attempts to update a primary key or a non-existent value.
func (db *DB) execUpdate(stmt *StmtUpdate) (count int, err error) {

	schema, err := db.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}

	iter, err := db.execCond(&schema, stmt.cond)
	if err != nil {
		return 0, err
	}

	for ; err == nil && iter.Valid(); err = iter.Next() {
		row := iter.Row()

		updates := make([]NamedCell, len(stmt.value))
		for i, assign := range stmt.value {
			cell, err := evalExpr(&schema, row, assign.expr)
			if err != nil {
				return 0, err
			}
			updates[i] = NamedCell{column: assign.column, value: *cell}
		}
		if err = fillNonPKey(&schema, updates, row); err != nil {
			return 0, err
		}
		updated, err := db.Update(&schema, row)
		if err != nil {
			return 0, err
		}
		if updated {
			count++
		}
	}
	if err != nil {
		return 0, err
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

	iter, err := db.execCond(&schema, stmt.cond)
	if err != nil {
		return 0, err
	}

	for ; err == nil && iter.Valid(); err = iter.Next() {
		row := iter.Row()
		updated, err := db.Delete(&schema, row)
		if err != nil {
			return 0, err
		}
		if updated {
			count++
		}
	}
	if err != nil {
		return 0, err
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

// execCond is the main entry point for the query optimizer.
// It takes the raw WHERE condition, converts it into a fast RangeReq,
// and immediately executes the scan to get an iterator.
func (db *DB) execCond(schema *Schema, cond interface{}) (*RowIterator, error) {
	// try to build a fast shortcut
	req, err := makeRange(schema, cond)
	if err != nil {
		return nil, err
	}
	// execute a fast KV store scan using the shortcut
	return db.Range(schema, req)
}

// extractPKey checks if the exact-match query actually uses the Primary Key.
// If the PK is 'id', but the query is 'WHERE name', this will return false.
func extractPKey(schema *Schema, pkey []NamedCell) (cells []Cell, ok bool) {
	if len(schema.PKey) != len(pkey) {
		return nil, false
	}
	for _, idx := range schema.PKey {
		col := schema.Cols[idx]
		i := slices.IndexFunc(pkey, func(e NamedCell) bool {
			return col.Name == e.column && col.Type == e.value.Type
		})
		if i < 0 {
			return nil, false
		}
		cells = append(cells, pkey[i].value)
	}
	return cells, true
}

// makeRange acts as the brain. It looks at the query and decides
// what kind of fast scan to build (an exact match or a range).
func makeRange(schema *Schema, cond interface{}) (*RangeReq, error) {
	// case 1: this is an exact match (ex. id = 5)
	if keys, ok := matchAllEq(cond, nil); ok {
		// if it lines up perfectly with the primary key than a range struct for it
		if pkey, ok := extractPKey(schema, keys); ok {
			return &RangeReq{
				StartCmp: OP_GE,
				StopCmp:  OP_LE,
				Start:    pkey,
				Stop:     pkey,
			}, nil
		}
	}
	// case 2: this is a range ex 5 < id < 10
	if req, ok := matchRange(schema, cond); ok {
		return req, nil
	}

	// default can't optimize this type of query yet so fail
	return nil, errors.New("unimplemented WHERE")
}

// Range returns a RowIterator for the specified boundaries and scan direction.
// It bootstraps the iterator by decoding the first valid KV pair into a Row.
func (db *DB) Range(schema *Schema, req *RangeReq) (*RowIterator, error) {
	// turn the 'Start' and 'Stop' values into byte arrays for the KV store
	start := EncodeKeyPrefix(schema, req.Start, suffixPositive(req.StartCmp))
	stop := EncodeKeyPrefix(schema, req.Stop, suffixPositive(req.StopCmp))
	desc := isDescending(req.StartCmp)
	// call the lower level Key-Value store to actually get the data
	iter, err := db.KV.Range(start, stop, desc)

	if err != nil {
		return nil, err
	}
	// create an empty row and decode the very first result into it
	row := schema.NewRow()

	isValid, err := decodeKVIter(schema, iter, row)
	if err != nil {
		return nil, err
	}
	
	// return an iterator so the program can loop through all the results
	return &RowIterator{schema: schema, iter: iter, valid: isValid, row: row}, nil
}

// asNameList turns a tree node into a list of strings (Column Names).
// It handles single columns "id" or grouped tuples "(a, b)".
func asNameList(expr interface{}) (out []string, ok bool) {
	switch e := expr.(type) {
	case string:
		return []string{e}, true
	case *ExprTuple:
		for _, kid := range e.kids {
			if s, ok := kid.(string); ok {
				out = append(out, s)
			} else {
				return nil, false
			}
		}
		return out, true
	}
	return nil, false
}

// asCellList turns a tree node into a list of Cells (Values).
// It handles single values "5" or grouped tuples "(1, 2)".
func asCellList(expr interface{}) (out []Cell, ok bool) {
	switch e := expr.(type) {
	case *Cell:
		return []Cell{*e}, true
	case *ExprTuple:
		for _, kid := range e.kids {
			if s, ok := kid.(*Cell); ok {
				out = append(out, *s)
			} else {
				return nil, false
			}
		}
		return out, true
	}
	return nil, false
}

// matchCmp tears apart a single inequality (like "id > 5").
// It figures out the operator (>), the column names (id), and the values (5).
func matchCmp(cond interface{}) (ExprOp, []string, []Cell, bool) {
	binop, ok := cond.(*ExprBinOp)
	if !ok {
		return 0, nil, nil, false
	}
	switch binop.op {
	// only care about greater-than or less-than here
	case OP_LE, OP_GE, OP_LT, OP_GT:
	default:
		return 0, nil, nil, false
	}

	op := binop.op
	left, right := binop.left, binop.right
	// check if the left side is a column name.
	names, ok := asNameList(left)
	// if its not than normalize it to be left sided
	if !ok {
		left, right = right, left
		names, ok = asNameList(left)
		switch op {
		case OP_LE:
			op = OP_GE
		case OP_GE:
			op = OP_LE
		case OP_LT:
			op = OP_GT
		case OP_GT:
			op = OP_LT
		}
	}
	if !ok {
		return 0, nil, nil, false
	}
	// get the actual values from the right side
	cells, ok := asCellList(right)
	if !ok {
		return 0, nil, nil, false
	}
	return op, names, cells, true
}

// isPKeyPrefix is a safety check. It ensures the program does not accidentally do a
// fast range scan on a column that isn't sorted (not a primary key).
func isPKeyPrefix(schema *Schema, cols []string, cells []Cell) bool {
	if len(cols) != len(cells) || len(cols) > len(schema.Cols) {
		return false
	}
	for i := range cols {
		col := schema.Cols[schema.PKey[i]]
		if col.Name != cols[i] || col.Type != cells[i].Type {
			return false
		}
	}
	return true
}

// matchRange looks for inequalities (>, <, >=, <=) and AND statements.
func matchRange(schema *Schema, cond interface{}) (*RangeReq, bool) {
	binop, ok := cond.(*ExprBinOp)
	// case 1: It's an AND statement (e.g., id > 5 AND id < 10)
	if ok && binop.op == OP_AND {
		// parse the left side
		op1, cols1, cells1, ok := matchCmp(binop.left)
		if !ok || !isPKeyPrefix(schema, cols1, cells1) {
			return nil, false
		}
		// parse the right side
		op2, cols2, cells2, ok := matchCmp(binop.left)
		if !ok || !isPKeyPrefix(schema, cols2, cells2) {
			return nil, false
		}
		// making sure one of them is a start and the other is a stop
		if isDescending(op1) != isDescending(op2) {
			return nil, false
		}
		// if they are backwards than swap so start is always before a stop
		if isDescending(op1) {
			op1, op2, cells1, cells2 = op2, op1, cells2, cells1
		}
		// give back final bounds
		return &RangeReq{
			StartCmp: op1,
			StopCmp:  op2,
			Start:    cells1,
			Stop:     cells2,
		}, true
		// case 2: It's a single condition (e.g., id > 5)
	} else if ok {
		op1, cols1, cells1, ok := matchCmp(cond)
		if !ok || !isPKeyPrefix(schema, cols1, cells1) {
			return nil, false
		}
		// only have a start, so its open bounded
		op2 := OP_LE
		if isDescending(op1) {
			op2 = OP_GE
		}
		return &RangeReq{
			StartCmp: op1,
			StopCmp:  op2,
			Start:    cells1,
			Stop:     nil, //make sure to go until the end
		}, true
	}
	return nil, false
}
