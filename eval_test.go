package kvdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testEval(t *testing.T, schema *Schema, row Row, s string, expected Cell) {
	p := NewParser(s)
	expr, err := p.parseExpr()
	require.Nil(t, err, "failed to parse: "+s)
	require.True(t, p.isEnd(), "parser did not consume entire string: "+s)

	out, err := evalExpr(schema, row, expr)
	require.Nil(t, err, "eval failed for: "+s)
	assert.Equal(t, expected, *out, "mismatch on query: "+s)
}

// testEvalError ensures that invalid queries properly return errors instead of panicking.
func testEvalError(t *testing.T, schema *Schema, row Row, s string, expectedErr string) {
	p := NewParser(s)
	expr, err := p.parseExpr()
	require.Nil(t, err, "failed to parse: "+s)

	_, err = evalExpr(schema, row, expr)
	require.NotNil(t, err, "expected an error but got none for: "+s)
	assert.Contains(t, err.Error(), expectedErr, "error message mismatch on query: "+s)
}

func makeCell(v interface{}) Cell {
	switch val := v.(type) {
	case int:
		return Cell{Type: TypeI64, I64: int64(val)}
	case string:
		return Cell{Type: TypeStr, Str: []byte(val)}
	default:
		panic("unreachable")
	}
}

func makeRow(vs ...interface{}) (row Row) {
	for _, v := range vs {
		row = append(row, makeCell(v))
	}
	return row
}

func TestEval(t *testing.T) {
	schema := &Schema{
		Table: "t",
		Cols: []Column{
			{"str1", TypeStr},
			{"str2", TypeStr},
			{"num1", TypeI64},
			{"num2", TypeI64},
			{"zero", TypeI64},
		},
		PKey: []int{0},
	}

	// Data: str1="apple", str2="banana", num1=10, num2=20, zero=0
	row := makeRow("apple", "banana", 10, 20, 0)

	// arithmetic ops (+, -, *, /)
	testEval(t, schema, row, "num1 + num2", makeCell(30))
	testEval(t, schema, row, "num2 - num1", makeCell(10))
	testEval(t, schema, row, "num1 * num2", makeCell(200))
	testEval(t, schema, row, "num2 / num1", makeCell(2))
	testEval(t, schema, row, "num1 + num2 * 2", makeCell(50)) // Precedence test: 10 + (20 * 2)

	// string concat
	testEval(t, schema, row, "str1 + str2", makeCell("applebanana"))
	testEval(t, schema, row, "str1 + ' pie'", makeCell("apple pie"))

	//(-x, NOT x)
	testEval(t, schema, row, "-num1", makeCell(-10))
	testEval(t, schema, row, "- -num1", makeCell(10)) // Double negation
	testEval(t, schema, row, "NOT zero", makeCell(1)) // 0 becomes 1
	testEval(t, schema, row, "NOT num1", makeCell(0)) // 10 becomes 0
	testEval(t, schema, row, "NOT NOT num1", makeCell(1))

	// Integer Comparisons
	testEval(t, schema, row, "num1 = 10", makeCell(1))
	testEval(t, schema, row, "num1 = num2", makeCell(0))
	testEval(t, schema, row, "num1 != num2", makeCell(1))
	testEval(t, schema, row, "num1 < num2", makeCell(1))
	testEval(t, schema, row, "num2 > num1", makeCell(1))
	testEval(t, schema, row, "num1 <= 10", makeCell(1))
	testEval(t, schema, row, "num2 >= 20", makeCell(1))

	// String Comparisons
	testEval(t, schema, row, "str1 = 'apple'", makeCell(1))
	testEval(t, schema, row, "str1 != str2", makeCell(1))
	testEval(t, schema, row, "str1 < str2", makeCell(1)) // 'a' comes before 'b'
	testEval(t, schema, row, "str2 > str1", makeCell(1))

	// AND OR
	testEval(t, schema, row, "num1 AND num2", makeCell(1)) // Non-zeros are true
	testEval(t, schema, row, "num1 AND zero", makeCell(0))
	testEval(t, schema, row, "zero OR num2", makeCell(1))
	testEval(t, schema, row, "zero OR 0", makeCell(0))

	// (10 + 20 > 25) AND ("apple" != "banana")  => (30 > 25) AND 1 => 1 AND 1 => 1
	testEval(t, schema, row, "(num1 + num2 > 25) AND (str1 != str2)", makeCell(1))

	// NOT (10 > 20) OR (0 AND 10) => NOT(0) OR 0 => 1 OR 0 => 1
	testEval(t, schema, row, "NOT (num1 > num2) OR (zero AND num1)", makeCell(1))

	// Missing Column
	testEvalError(t, schema, row, "ghost_column + 1", "not found in the given schema")

	// Type Mismatches
	testEvalError(t, schema, row, "str1 + num1", "Cell type is not matching")
	testEvalError(t, schema, row, "str1 = num1", "Cell type is not matching")

	// Unary Type Errors
	testEvalError(t, schema, row, "-str1", "CANT USE A NON INT")
	testEvalError(t, schema, row, "NOT str1", "CANT USE A NON INT")

	// Logical Operator Type Errors
	testEvalError(t, schema, row, "str1 AND str2", "Requires integer operands")

	// Division by zero
	testEvalError(t, schema, row, "num1 / zero", "Division by 0 not allowed")
	testEvalError(t, schema, row, "num1 / (num2 - 20)", "Division by 0 not allowed") // dynamic zero
}
