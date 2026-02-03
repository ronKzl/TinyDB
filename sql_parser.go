package kvdb

import (
	"errors"
	"strconv"
	"strings"
)

type Parser struct {
	buf string
	pos int
}

type StmtSelect struct {
	table string
	cols  []string
	keys  []NamedCell
}

type NamedCell struct {
	column string
	value  Cell
}

type StmtCreatTable struct {
	table string
	cols  []Column
	pkey  []string
}

type StmtInsert struct {
	table string
	value []Cell
}

type StmtUpdate struct {
	table string
	keys  []NamedCell
	value []NamedCell
}

type StmtDelete struct {
	table string
	keys  []NamedCell
}

func NewParser(s string) Parser {
	return Parser{buf: s, pos: 0}
}

func isSpace(ch byte) bool {
	switch ch {
	case '\t', '\n', '\v', '\f', '\r', ' ':
		return true
	}
	return false
}

func isAlpha(ch byte) bool {
	return 'a' <= (ch|32) && (ch|32) <= 'z'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func isNameStart(ch byte) bool {
	return isAlpha(ch) || ch == '_'
}

func isNameContinue(ch byte) bool {
	return isAlpha(ch) || isDigit(ch) || ch == '_'
}

func isSeparator(ch byte) bool {
	return ch < 128 && !isNameContinue(ch)
}

func (p *Parser) skipSpaces() {
	for p.pos < len(p.buf) && isSpace(p.buf[p.pos]) {
		p.pos += 1
	}
}

func (p *Parser) isEnd() bool {
	p.skipSpaces()
	return p.pos >= len(p.buf)
}

func (p *Parser) tryKeyword(kws ...string) bool {
	initialPos := p.pos
	for _, kw := range(kws) {
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

func (p *Parser) tryPunctuation(tok string) bool {
	p.skipSpaces()
	if !(p.pos+len(tok) <= len(p.buf) && p.buf[p.pos:p.pos+len(tok)] == tok) {
		return false
	}
	p.pos += len(tok)
	return true
}

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

func (p *Parser) parseSelect(out *StmtSelect) error {
	for !p.tryKeyword("FROM") {
		if len(out.cols) > 0 && !p.tryPunctuation(",") {
			return errors.New("expect comma")
		}
		if name, ok := p.tryName(); ok {
			out.cols = append(out.cols, name)
		} else {
			return errors.New("expect column")
		}
	}

	if len(out.cols) == 0 {
		return errors.New("expect colum list")
	}

	var ok bool

	if out.table, ok = p.tryName(); !ok {
		return errors.New("expect table name")
	}

	return p.parseWhere(&out.keys)
}

func (p *Parser) parseWhere(out *[]NamedCell) error {
	if !p.tryKeyword("WHERE") {
		return errors.New("expect keyword WHERE")
	}

	for !p.tryPunctuation(";") {
		if len(*out) > 0 && !p.tryKeyword("AND") {
			return errors.New("expect AND")
		}
		var res NamedCell
		if err := p.parseEqual(&res); err != nil {
			return err
		}
		*out = append(*out, res)
	}
	if len(*out) == 0 {
		return errors.New("expect WHERE clause")
	}

	return nil
}

func (p *Parser) parseCreateTable(out *StmtCreatTable) error {
	var ok bool 
	if out.table, ok = p.tryName(); !ok {
		return errors.New("CREATE TABLE: error reading table name")
	}

	if !p.tryPunctuation("(") {
		return errors.New("CREATE TABLE: (VARIABLE NAME DEFINTION) no opening (")
	}
	
	
	for !p.tryKeyword("PRIMARY","KEY") {
		var col Column
		
			
		if col.Name, ok = p.tryName(); !ok {
			return errors.New("CREATE TABLE: error reading variable name")
		}
			
		var varType string
		if varType, ok = p.tryName(); !ok {
			return errors.New("CREATE TABLE: error reading variable type")
		}
		if strings.EqualFold(varType,"int64") {
			col.Type = TypeI64
		} else if strings.EqualFold(varType,"string") {
			col.Type = TypeStr
		} else {
			return errors.New("CREATE TABLE: incompativle variable type")
		}
				
		
		out.cols = append(out.cols, col)
		p.tryPunctuation(",")
	}

	if !p.tryPunctuation("(") {
		return errors.New("CREATE TABLE: (PRIMARY KEY DEFINITION) no opening (")
	}
	
	for !p.tryPunctuation(")"){
		p.tryPunctuation(",") 
		var PK string

		if PK, ok = p.tryName(); !ok {
			return errors.New("CREATE TABLE: error reading PK name")
		}
		found := false
		for _, col := range(out.cols) {
			if strings.EqualFold(col.Name,PK) {
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

func (p *Parser) parseUpdate(out *StmtUpdate) error {

	var ok bool
	if out.table, ok = p.tryName(); !ok {
		return errors.New("UPDATE: error parsing table name")
	}
	
	if !p.tryKeyword("SET") {
		return errors.New("UPDATE: missing SET")
	}
	
	for {
		cel := NamedCell{}
		if err := p.parseEqual(&cel); err != nil {
			return err
		}
		out.value = append(out.value, cel)
		if !p.tryPunctuation(","){
			break
		}
	}

	return p.parseWhere(&out.keys)
}

func (p *Parser) parseDelete(out *StmtDelete) error {
	var ok bool
	if out.table, ok = p.tryName(); !ok {
		return errors.New("DELETE: error parsing table name")
	}
	return p.parseWhere(&out.keys)
}

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
