package kvdb

import (
	"errors"
	"strconv"
	"strings"
)

// Parser contains the entire user buffer input as well as the position of the parser in the buffer.
type Parser struct {
	buf string
	pos int
}

// StmtSelect is a SELECT query struct.
// It holds the table name as well as the NamedCell keys slice and the cols interface slice.
type StmtSelect struct {
	table string
	cols  []interface{} // ExprUnOp | ExprBinOp | string | *Cell
	cond  interface{}
}

// NamedCell contains the column name and the Cell in that column.
type NamedCell struct {
	column string
	value  Cell
}

// StmtCreatTable is a CREATE query struct.
// It holds the table name as well as the columns and primary keys that will make it up.
type StmtCreatTable struct {
	table string
	cols  []Column
	pkey  []string
}

// StmtInsert is an INSERT query struct.
// It holds the table name as well as the Cell that is being inserted into it.
type StmtInsert struct {
	table string
	value []Cell
}

// StmtUpdate is an UPDATE query struct.
// It holds the table name as well as slices for the keys and values being updated.
type StmtUpdate struct {
	table string
	cond  interface{}
	value []ExprAssign
}

// ExprAssign is an assignment query struct.
// It holds the column name as well as the expression assigned to that column.
type ExprAssign struct {
	column string
	expr   interface{} // ExprUnOp | ExprBinOp | string | *Cell
}

// StmtDelete is a DELETE query struct.
// It holds the table name as well as the keys meant for deletion.
type StmtDelete struct {
	table string
	cond  interface{}
}

// ExprBinOp is a tree structure that holds the operations of an inputted user query
type ExprBinOp struct {
	op ExprOp
	// Can be any type but I am using a string as column name, Cell for a constant, ExprBinOp for a nested expr.
	left  interface{}
	right interface{}
}

// ExprUnOp is a simple structure to express NOT x and -x operators having only a single child node.
type ExprUnOp struct {
	op  ExprOp      // OP_NEG or OP_NOT
	kid interface{} // Can be any type, a Cell, variable, ExprUnOp or ExprBinOp
}

type ExprTuple struct {
	kids []interface{}
}

type ExprOp uint8

const (
	OP_ADD ExprOp = 1  // +
	OP_SUB ExprOp = 2  // -
	OP_MUL ExprOp = 3  // *
	OP_DIV ExprOp = 4  // //
	OP_NEG ExprOp = 5  // -x
	OP_LE  ExprOp = 12 // <= smaller then or equal
	OP_GE  ExprOp = 13 // >= greater then or equal
	OP_LT  ExprOp = 14 // <| smaller then
	OP_GT  ExprOp = 15 // |> greater then
	OP_EQ  ExprOp = 16 // =
	OP_NE  ExprOp = 17 // !=/ <>
	OP_OR  ExprOp = 50 // OR
	OP_AND ExprOp = 51 // AND
	OP_NOT ExprOp = 52 // NOT
)

// NewParser allocates a Parser for a given input string.
func NewParser(s string) Parser {
	return Parser{buf: s, pos: 0}
}

// parseAtom parses a single operand, which can be either a parentheses expression,
// a column name (string) or a constant value (*Cell).
func (p *Parser) parseAtom() (expr interface{}, err error) {
	// parentheses have highest precedence
	if p.tryPunctuation("(") {
		p.pos--
		return p.parseTuple()
	}
	if name, ok := p.tryName(); ok {
		return name, nil
	}
	cell := &Cell{}
	if err := p.parseValue(cell); err != nil {
		return nil, err
	}
	return cell, nil
}

// parseTuple parses tuple operands (a, b) into an populates the tree.
func (p *Parser) parseTuple() (expr interface{}, err error) {
	kids := []interface{}{}
	err = p.parseCommaList(func() error {
		expr, err := p.parseExpr()
		if err != nil {
			return err
		}
		kids = append(kids, expr)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(kids) == 0 {
		return nil, errors.New("empty tuple")
	} else if len(kids) == 1 {
		return kids[0], nil
	} else {
		return &ExprTuple{kids}, nil
	}

}

// parseCommaList tries to parse a list of comma separated values from the buffer,
// takes in a function to call that will handle the correct value parsing.
func (p *Parser) parseCommaList(item func() error) error {
	if !p.tryPunctuation("(") {
		return errors.New("expect (")
	}
	comma := false
	for !p.tryPunctuation(")") {
		if comma && !p.tryPunctuation(",") {
			return errors.New("expect ,")
		}
		comma = true
		if err := item(); err != nil {
			return err
		}
	}
	return nil
}

// mapOperators converts a string representation of an operator into its corresponding ExprOp type.
// TODO: tech debt to refactor - potentially if other operators are added in the future will just get ouf of control.
func mapOperators(op string) ExprOp {
	switch strings.ToLower(op) {
	case ">=":
		return OP_GE
	case "<=":
		return OP_LE
	case "+":
		return OP_ADD
	case "-":
		return OP_SUB
	case "*":
		return OP_MUL
	case "/":
		return OP_DIV
	case ">":
		return OP_GT
	case "<":
		return OP_LT
	case "!=":
		return OP_NE
	case "<>":
		return OP_NE
	case "=":
		return OP_EQ
	case "or":
		return OP_OR
	case "and":
		return OP_AND
	case "not":
		return OP_NOT
	default:
		panic("non matching operator")
	}
}

// parseExpr parses a buffer expression according to
// order of operations (from lowest to highest) into a left-associative binary expression tree.
func (p *Parser) parseExpr() (interface{}, error) {
	return p.parseOr()
}

// parseBinOp is a generic helper that handles left-associative binary operators.
// It accumulates expressions into a tree, ensuring that the operators provided
// in the current level have lower precedence than those in the innerPrecedence call.
func (p *Parser) parseBinOp(tokens []string, innerPrecedence func() (interface{}, error)) (interface{}, error) {
	// accumulator
	result, err := innerPrecedence()

	if err != nil {
		return nil, err
	}

	for {
		found := false
		for _, op := range tokens {
			if !p.tryPunctuation(op) && !p.tryKeyword(op) {
				continue
			}

			rightValue, err := innerPrecedence()
			if err != nil {
				return nil, err
			}
			result = &ExprBinOp{left: result, op: mapOperators(op), right: rightValue}
			found = true
			break

		}

		if !found {
			break
		}
	}
	return result, nil
}

// parseOr handles the logical OR operator, which has the lowest precedence level.
func (p *Parser) parseOr() (interface{}, error) {
	return p.parseBinOp([]string{"OR"}, p.parseAnd)
}

// parseAnd handles logical AND operations.
func (p *Parser) parseAnd() (interface{}, error) {
	return p.parseBinOp([]string{"AND"}, p.parseNot)
}

// parseNot handles the unary NOT operator. It is right-associative and
// can be nested (e.g., NOT NOT a).
func (p *Parser) parseNot() (expr interface{}, err error) {
	if p.tryKeyword("NOT") {
		if expr, err = p.parseNot(); err != nil {
			return nil, err
		}
		return &ExprUnOp{op: OP_NOT, kid: expr}, nil
	} else {
		return p.parseCmp()
	}
}

// parseCmp handles comparison operators like =, !=, <, and >.
func (p *Parser) parseCmp() (interface{}, error) {
	return p.parseBinOp(
		[]string{"=", "!=", "<>", "<=", ">=", "<", ">"},
		p.parseAdd)
}

// parseAdd handles addition and subtraction operations.
func (p *Parser) parseAdd() (interface{}, error) {
	return p.parseBinOp([]string{"+", "-"}, p.parseMul)
}

// parseMul handles high-precedence arithmetic operators: multiplication and division.
func (p *Parser) parseMul() (interface{}, error) {
	return p.parseBinOp([]string{"*", "/"}, p.parseNeg)
}

// parseNeg handles unary negation. It reports whether a negative sign is present
// and recurses to handle cases like --a, otherwise it parses an atomic value.
func (p *Parser) parseNeg() (expr interface{}, err error) {
	if p.tryPunctuation("-") {
		if expr, err = p.parseNeg(); err != nil {
			return nil, err
		}
		return &ExprUnOp{op: OP_NEG, kid: expr}, nil
	} else {
		return p.parseAtom()
	}
}

// isSpace is a helper function that determines whether the given byte is a space character.
func isSpace(ch byte) bool {
	switch ch {
	case '\t', '\n', '\v', '\f', '\r', ' ':
		return true
	}
	return false
}

// isAlpha is a helper function that reports whether the given byte is a letter.
func isAlpha(ch byte) bool {
	return 'a' <= (ch|32) && (ch|32) <= 'z'
}

// isDigit is a helper function that reports whether the given byte is a number char.
func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

// isNameStart is a helper function that reports whether the given byte is a valid start for a variable name.
func isNameStart(ch byte) bool {
	return isAlpha(ch) || ch == '_'
}

// isNameContinue is a helper function that reports whether the given byte is a valid char in a variable name.
func isNameContinue(ch byte) bool {
	return isAlpha(ch) || isDigit(ch) || ch == '_'
}

// isSeparator is a helper function that reports whether the given byte is a separator character.
func isSeparator(ch byte) bool {
	return ch < 128 && !isNameContinue(ch)
}

// skipSpaces is a helper function that puts the position in the buffer to the next non-whitespace byte.
func (p *Parser) skipSpaces() {
	for p.pos < len(p.buf) && isSpace(p.buf[p.pos]) {
		p.pos += 1
	}
}

// isEnd is a helper function that checks whether the position in the buffer has reached its end.
func (p *Parser) isEnd() bool {
	p.skipSpaces()
	return p.pos >= len(p.buf)
}

// tryKeyword is a helper function reports whether or not the given string words are found in the buffer.
//
// If they are found the buffer position is adjusted accordingly to the end of the last given word.
func (p *Parser) tryKeyword(kws ...string) bool {
	initialPos := p.pos
	for _, kw := range kws {
		p.skipSpaces()
		if len(p.buf)-p.pos < len(kw) {
			p.pos = initialPos
			return false
		}

		startPos := p.pos
		endPos := startPos + len(kw)

		if !strings.EqualFold(p.buf[startPos:endPos], kw) {
			p.pos = initialPos
			return false
		}

		if endPos < len(p.buf) && !isSpace(p.buf[endPos]) && !isSeparator(p.buf[endPos]) {
			p.pos = initialPos
			return false
		}

		p.pos = endPos
	}

	return true
}

// tryPunctuation is a helper function that given a punctuation character reports whether it is found as the next char in
// the buffer.
//
// If its found in the buffer it increments the buffer position to after that character.
func (p *Parser) tryPunctuation(tok string) bool {
	p.skipSpaces()
	if !(p.pos+len(tok) <= len(p.buf) && p.buf[p.pos:p.pos+len(tok)] == tok) {
		return false
	}
	p.pos += len(tok)
	return true
}

// tryName is a helper function that returns whether or not the buffer was able to parse a variable name as the next input in the buffer.
//
// Returns the variable name from the buffer and adjusts the position of the Parser in the buffer if it was successful.
func (p *Parser) tryName() (string, bool) {
	initialPos := p.pos
	p.skipSpaces()
	startPos := p.pos

	if !isNameStart(p.buf[p.pos]) {
		p.pos = initialPos
		return "", false
	}

	for p.pos < len(p.buf) && isNameContinue(p.buf[p.pos]) {
		p.pos += 1
	}

	return p.buf[startPos:p.pos], true
}

// parseValue is a helper function that attempts to extract a value from the buffer and populate a Cell with it.
func (p *Parser) parseValue(out *Cell) error {
	p.skipSpaces()
	if p.pos >= len(p.buf) {
		return errors.New("expect value")
	}
	ch := p.buf[p.pos]
	if ch == '"' || ch == '\'' {
		return p.parseString(out)
	} else if isDigit(ch) || ch == '-' || ch == '+' {
		return p.parseInt(out)
	} else {
		return errors.New("expect value")
	}
}

// parseString is a helper function that parses user input value of type string in the buffer.
//
// If successful the position of the buffer is updated and the Cell type will hold the user input string value.
func (p *Parser) parseString(out *Cell) error {

	quoteCloser := p.buf[p.pos]
	p.pos += 1

	cur := p.pos
	res := []byte{}

	escape := false

	for cur < len(p.buf) && (escape || (p.buf[cur] != quoteCloser)) {
		if !escape && p.buf[cur] == '\\' {
			escape = true
			cur += 1
			continue
		} else if escape {
			escape = false
		}
		res = append(res, byte(p.buf[cur]))
		cur += 1

	}

	if cur >= len(p.buf) {
		return errors.New("string not terminated")
	}

	p.pos = cur + 1
	out.Type = TypeStr
	out.Str = res

	return nil
}

// parseInt is a helper function that parses user input value of type integer in the buffer.
//
// If successful the position of the buffer is updated and the Cell type will hold the user input integer value.
func (p *Parser) parseInt(out *Cell) (err error) {
	start := p.pos
	cur := p.pos

	if cur < len(p.buf) && (p.buf[cur] == '+' || p.buf[cur] == '-') {
		cur += 1
	}

	if cur >= len(p.buf) || !isDigit(p.buf[cur]) {
		return errors.New("Invalid Integer")
	}

	for cur < len(p.buf) && isDigit(p.buf[cur]) {
		cur += 1
	}

	p.pos = cur
	out.Type = TypeI64
	val, err := strconv.ParseInt(p.buf[start:cur], 10, 64)
	if err != nil {
		return err
	}
	out.I64 = int64(val)
	return nil
}

// parseEqual is a helper function that attempts to parse an equality assignment from the buffer.
//
// Example: "a = 2", if successful buffer position will be updated and NamedCell type will be updated with the column and value.
func (p *Parser) parseEqual(out *NamedCell) error {
	var ok bool
	out.column, ok = p.tryName()
	if !ok {
		return errors.New("expect column")
	}
	if !p.tryPunctuation("=") {
		return errors.New("expect =")
	}

	return p.parseValue(&out.value)
}

// parseSelect attempts to parse the input buffer for a SELECT FROM SQL statement structure.
//
// If successful it will populate the StmtSelect structure with the table name, db columns and db PKs.
// Else will return an error to user to indicate that their query was formatted incorrectly.
func (p *Parser) parseSelect(out *StmtSelect) (err error) {
	for !p.tryKeyword("FROM") {
		if len(out.cols) > 0 && !p.tryPunctuation(",") {
			return errors.New("expect comma")
		}
		expr, err := p.parseExpr()
		if err != nil {
			return err
		}
		out.cols = append(out.cols, expr)
	}

	if len(out.cols) == 0 {
		return errors.New("expect column list")
	}

	var ok bool

	if out.table, ok = p.tryName(); !ok {
		return errors.New("expect table name")
	}

	out.cond, err = p.parseWhere()
	return err
}

// parseWhere attempts to parse the WHERE clause of a SQL query in the buffer.
//
// If successful will populate the NameCell slice with all the columns and their respective values from the buffer.
func (p *Parser) parseWhere() (expr interface{}, err error) {
	if !p.tryKeyword("WHERE") {
		return nil, errors.New("expect keyword WHERE")
	}

	expr, err = p.parseExpr()
	if err != nil {
		return nil, err
	}

	if !p.tryPunctuation(";") {
		return nil, errors.New("expect ;")
	}

	return expr, nil
}

// parseCreateTable attempts to parse the input buffer for a CREATE TABLE SQL statement structure.
//
// If successful it will populate the StmtCreatTable structure with the table name, db columns and db PKs.
// Else will return an error to user to indicate that their query was formatted incorrectly.
func (p *Parser) parseCreateTable(out *StmtCreatTable) error {
	var ok bool
	if out.table, ok = p.tryName(); !ok {
		return errors.New("CREATE TABLE: error reading table name")
	}

	if !p.tryPunctuation("(") {
		return errors.New("CREATE TABLE: (VARIABLE NAME DEFINITION) no opening (")
	}

	for !p.tryKeyword("PRIMARY", "KEY") {
		var col Column

		if col.Name, ok = p.tryName(); !ok {
			return errors.New("CREATE TABLE: error reading variable name")
		}

		var varType string
		if varType, ok = p.tryName(); !ok {
			return errors.New("CREATE TABLE: error reading variable type")
		}
		if strings.EqualFold(varType, "int64") {
			col.Type = TypeI64
		} else if strings.EqualFold(varType, "string") {
			col.Type = TypeStr
		} else {
			return errors.New("CREATE TABLE: incompatible variable type")
		}

		out.cols = append(out.cols, col)
		p.tryPunctuation(",")
	}

	if !p.tryPunctuation("(") {
		return errors.New("CREATE TABLE: (PRIMARY KEY DEFINITION) no opening (")
	}

	for !p.tryPunctuation(")") {
		p.tryPunctuation(",")
		var PK string

		if PK, ok = p.tryName(); !ok {
			return errors.New("CREATE TABLE: error reading PK name")
		}
		found := false
		for _, col := range out.cols {
			if strings.EqualFold(col.Name, PK) {
				out.pkey = append(out.pkey, PK)
				found = true
			}
		}

		if !found {
			return errors.New("CREATE TABLE: error given PK not found in table definition")
		}

	}

	if !p.tryPunctuation(")") {
		return errors.New("CREATE TABLE: no closing )")
	}

	if !p.tryPunctuation(";") {
		return errors.New("CREATE TABLE: no closing ;")
	}

	return nil
}

// parseInsert attempts to parse the input buffer for an INSERT INTO SQL statement structure.
//
// If successful it will populate the StmtInsert structure with the table name, and Cell value being inserted.
// Else will return an error to user to indicate that their query was formatted incorrectly.
func (p *Parser) parseInsert(out *StmtInsert) error {
	var ok bool
	if out.table, ok = p.tryName(); !ok {
		return errors.New("INSERT INTO: error parsing table name")
	}

	if ok = p.tryKeyword("VALUES"); !ok {
		return errors.New("INSERT INTO: missing VALUES declaration")
	}

	if ok = p.tryPunctuation("("); !ok {
		return errors.New("INSERT INTO: missing ( bracket in value declaration")
	}

	for !p.tryPunctuation(")") {
		var cell Cell
		if err := p.parseValue(&cell); err != nil {
			return err
		}
		out.value = append(out.value, cell)
		p.tryPunctuation(",")
	}

	if !p.tryPunctuation(";") {
		return errors.New("INSERT INTO: missing ;")
	}
	return nil
}

// parseUpdate attempts to parse the input buffer for a UPDATE SQL statement structure.
//
// If successful it will populate the StmtUpdate structure with the table name, and NamedCell slice of the keys and values being updated.
// Else will return an error to user to indicate that their query was formatted incorrectly.
func (p *Parser) parseUpdate(out *StmtUpdate) (err error) {

	var ok bool
	if out.table, ok = p.tryName(); !ok {
		return errors.New("UPDATE: error parsing table name")
	}

	if !p.tryKeyword("SET") {
		return errors.New("UPDATE: missing SET")
	}

	for {
		expr := ExprAssign{}
		if err := p.parseAssign(&expr); err != nil {
			return err
		}
		out.value = append(out.value, expr)
		if !p.tryPunctuation(",") {
			break
		}
	}
	if len(out.value) == 0 {
		return errors.New("UPDATE: expected assignment list")
	}
	out.cond, err = p.parseWhere()
	return err
}

// parseAssign attempts to parse a column name and an expression from the input buffer.
//
// If successful it will populate the passed in ExprAssign else will throw and error.
func (p *Parser) parseAssign(out *ExprAssign) (err error) {
	var ok bool
	out.column, ok = p.tryName()
	if !ok {
		return errors.New("expect column")
	}
	if !p.tryPunctuation("=") {
		return errors.New("expect =")
	}
	out.expr, err = p.parseExpr()
	return err
}

// parseDelete attempts to parse the input buffer for a DELETE SQL statement structure.
//
// If successful it will populate the StmtDelete structure with the table name, and NamedCell slice of the keys being deleted.
// Else will return an error to user to indicate that their query was formatted incorrectly.
func (p *Parser) parseDelete(out *StmtDelete) (err error) {
	var ok bool
	if out.table, ok = p.tryName(); !ok {
		return errors.New("DELETE: error parsing table name")
	}
	out.cond, err = p.parseWhere()
	return err
}

// parseStmt is a helper function that tries to determine what the buffer query is in order to execute the appropriate
// buffer parsing operation on the user buffer.
func (p *Parser) parseStmt() (out interface{}, err error) {
	if p.tryKeyword("SELECT") {
		stmt := &StmtSelect{}
		err = p.parseSelect(stmt)
		out = stmt
	} else if p.tryKeyword("CREATE", "TABLE") {
		stmt := &StmtCreatTable{}
		err = p.parseCreateTable(stmt)
		out = stmt
	} else if p.tryKeyword("INSERT", "INTO") {
		stmt := &StmtInsert{}
		err = p.parseInsert(stmt)
		out = stmt
	} else if p.tryKeyword("UPDATE") {
		stmt := &StmtUpdate{}
		err = p.parseUpdate(stmt)
		out = stmt
	} else if p.tryKeyword("DELETE", "FROM") {
		stmt := &StmtDelete{}
		err = p.parseDelete(stmt)
		out = stmt
	} else {
		err = errors.New("unknown statement")
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}
