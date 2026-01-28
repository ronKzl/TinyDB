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

func (p *Parser) tryKeyword(kw string) bool {
	initialPos := p.pos
	p.skipSpaces()

	if len(p.buf) - p.pos < len(kw) {
		p.pos = initialPos
		return false
	}

	startPos := p.pos
	endPos := startPos+len(kw)

	if !strings.EqualFold(p.buf[startPos:endPos], kw) {
		p.pos = initialPos
		return false
	}

	if endPos < len(p.buf) && !isSpace(p.buf[endPos]) && !isSeparator(p.buf[endPos]){
		p.pos = initialPos
		return false
	}
	
	p.pos = endPos
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

func (p *Parser) parseString(out *Cell) error{
	
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

func (p *Parser) parseInt(out *Cell) (err error){
	start := p.pos
	cur := p.pos + 1
	
	if cur >= len(p.buf) || !isDigit(p.buf[cur]) { return errors.New("Invalid Integer")}

	for cur < len(p.buf) && isDigit(p.buf[cur]){
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
