package kvdb

import (
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
