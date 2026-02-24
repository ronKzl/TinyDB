package kvdb

import (
	"bytes"
	"cmp"
	"errors"
	"slices"
	"strings"
)

// evalExpr is a recursive tree-walk interpreter that evaluates an expression
// against a specific row. It returns the computed *Cell result or an error
// if types are mismatched or columns are missing.
//
// TODO: Tech debt to split up functionality into multiple functions for readability. 
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
	case *ExprUnOp:
		child, err := evalExpr(schema, row, e.kid)
		if err != nil {
			return nil, err
		}
		switch e.op {
		case OP_NEG:
			if child.Type != TypeI64 {
				return nil, errors.New("FOR: NEGATION CANT USE A NON INT TYPE")
			}
			return &Cell{Type: TypeI64, I64: -child.I64}, nil
		case OP_NOT:
			if child.Type != TypeI64 {
				return nil, errors.New("FOR: NOT CANT USE A NON INT TYPE")
			}
			boolVal := int64(0)
			if child.I64 == 0 {
				boolVal = 1
			}
			return &Cell{Type: TypeI64, I64: boolVal}, nil
		default:
			panic("unreachable")
		}
	case *ExprBinOp:
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
		result := &Cell{}
		switch e.op {
		case OP_EQ, OP_NE, OP_GT, OP_LT, OP_GE, OP_LE:
			result.Type = TypeI64
			r := 0
			switch left.Type {
			case TypeI64:
				r = cmp.Compare(left.I64, right.I64)
			case TypeStr:
				r = bytes.Compare(left.Str, right.Str)
			default:
				panic("unreachable")
			}
			boolVal := false
			switch e.op {
			case OP_EQ:
				boolVal = (r == 0)
			case OP_NE:
				boolVal = (r != 0)
			case OP_LE:
				boolVal = (r <= 0)
			case OP_GE:
				boolVal = (r >= 0)
			case OP_LT:
				boolVal = (r < 0)
			case OP_GT:
				boolVal = (r > 0)
			}
			if boolVal {
				result.I64 = 1
			} else {
				result.I64 = 0
			}

		case OP_ADD, OP_SUB, OP_MUL, OP_DIV:
			result.Type = left.Type
			switch {
			case result.Type == TypeStr && e.op == OP_ADD:
				result.Str = slices.Concat(left.Str, right.Str)
			case result.Type == TypeI64 && e.op == OP_ADD:
				result.I64 = left.I64 + right.I64
			case result.Type == TypeI64 && e.op == OP_SUB:
				result.I64 = left.I64 - right.I64
			case result.Type == TypeI64 && e.op == OP_MUL:
				result.I64 = left.I64 * right.I64
			case result.Type == TypeI64 && e.op == OP_DIV:
				if right.I64 == 0 {
					return nil, errors.New("Division by 0 not allowed")
				}
				result.I64 = left.I64 / right.I64
			default:
				return nil, errors.New("Unknown operator type")
			}
		case OP_OR:
			result.Type = TypeI64
			if left.I64 != 0 || right.I64 != 0 {
				result.I64 = 1
			} else {
				result.I64 = 0
			}
		case OP_AND:
			if left.Type != TypeI64 || right.Type != TypeI64 {
				return nil, errors.New("Requires integer operands")
			}
			result.Type = TypeI64
			if left.I64 != 0 && right.I64 != 0 {
				result.I64 = 1
			} else {
				result.I64 = 0
			}
		default:
			return nil, errors.New("Unknown operator type")
		}
		return result, nil
	default:
		panic("unreachable")
	}
}
