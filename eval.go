package kvdb

import (
	"errors"
	"strings"
)

// evalExpr is a recursive tree-walk interpreter that evaluates an expression
// against a specific row. It returns the computed *Cell result or an error
// if types are mismatched or columns are missing.
func evalExpr(schema *Schema, row Row, expr interface{}) (*Cell, error) {
	switch e := expr.(type) {
	case string:
		// resolve column names to their actual values in the current row
		for idx, colName := range schema.Cols {
			if strings.EqualFold(colName.Name, e) {
				return &row[idx], nil
			}
		}
		return nil, errors.New("Column " + e + " not found in the given schema.")
	case *Cell:
		return e, nil
	case *ExprBinOp:
		// recursive step gather left and right and then perform the current tree operation on them
		left, err := evalExpr(schema, row, e.left)
		if err != nil {
			return nil, err
		}
		right, err := evalExpr(schema, row, e.right)
		if err != nil {
			return nil, err
		}
		// currently only strong typing is supported
		if left.Type != right.Type {
			return nil, errors.New("Cell type is not matching")
		}

		result := &Cell{Type: left.Type}
		switch {
		case result.Type == TypeStr && e.op == OP_ADD:
			result.Str = append(left.Str, right.Str...)
		case result.Type == TypeI64 && e.op == OP_ADD:
			result.I64 = left.I64 + right.I64
		case result.Type == TypeI64 && e.op == OP_SUB:
			result.I64 = left.I64 - right.I64
		default:
			return nil, errors.New("Unknown operator type")
		}
		return result, nil
	default:
		panic("unreachable")
	}
}
