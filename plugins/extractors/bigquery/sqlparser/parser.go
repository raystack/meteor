package sqlparser

import (
	"regexp"
	"strings"
)

var (
	queryCommentPatterns = regexp.MustCompile(`(--.*)|(((/\\*)+?[\\w\\W]*?(\\*/)+))`)

	// join patterns
	joinPatterns = regexp.MustCompile("" +
		"(?i)(?:ON)\\s*([a-zA-Z0-9@_\\.\\`-]*)\\s*\\=\\s*([a-zA-Z0-9@_\\.\\`-]*)" +
		"|" +
		"(?i)(?:USING)\\s*\\(\\s*([a-zA-Z0-9,@_\\s `-]*)\\s*\\)")

	// filter patterns
	whereBoolExpr               = "[a-zA-Z0-9@_\"\\',\\.\\x60-]*"
	whereBoolExprWithWhitespace = "[a-zA-Z0-9@_\"\\',\\s\\.\\x60-]*"
	whereBoolTerminals          = "" +
		whereBoolExpr + "\\s*(=|<|>|<=|>=|!=|<>)\\s*" + whereBoolExpr +
		"|" +
		whereBoolExpr + "\\s+(LIKE|NOT LIKE)\\s+" + whereBoolExpr +
		"|" +
		whereBoolExpr + "\\s+(BETWEEN|NOT BETWEEN)\\s+" + whereBoolExpr + "\\s+AND\\s+" + whereBoolExpr +
		"|" +
		whereBoolExpr + "\\s+IS (?:NOT)?\\s?(S_NULL|TRUE|FALSE)" +
		"|" +
		whereBoolExpr + "\\s+(IN|NOT IN)\\s?\\(" + whereBoolExprWithWhitespace + "\\)"
	whereRegex    = "(?i)(?:WHERE|HAVING)\\s+(" + whereBoolTerminals + ")(\\s+(AND|OR)\\s+(" + whereBoolTerminals + "))*"
	wherePatterns = regexp.MustCompile(whereRegex)
)

// ParseJoinConditions will return all join condition (ON and USING) in sql Query in a list of string
// where each of it is a join condition string
func ParseJoinConditions(sqlQuery string) (jcs []string) {
	sqlQuery = cleanUpQuery(sqlQuery)

	matches := joinPatterns.FindAllStringSubmatch(sqlQuery, -1)
	for _, match := range matches {

		const joinConditionFullIdx = 0
		jcs = append(jcs, match[joinConditionFullIdx])
	}

	return
}

// ParseFilterConditions will return all filter condition (WHERE and HAVING) in sql Query in a list of string
func ParseFilterConditions(sqlQuery string) (fcs []string) {
	sqlQuery = cleanUpQuery(sqlQuery)

	fcs = wherePatterns.FindAllString(sqlQuery, -1)
	return
}

func cleanUpQuery(s string) string {
	// remove comments from query
	matches := queryCommentPatterns.FindAllStringSubmatch(s, -1)
	for _, match := range matches {
		// replace full match
		s = strings.ReplaceAll(s, match[0], " ")
	}

	// cleaning leading and trailing whitespace
	s = strings.TrimSpace(s)
	// standarizing string
	s = strings.Join(strings.Fields(s), " ")
	// removing ; char
	s = strings.ReplaceAll(s, ";", "")

	return s
}
