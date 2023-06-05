package sqlparser

import (
	"regexp"
	"strings"
)

var (
	queryCommentPatterns = regexp.MustCompile(`(--.*)|(((/\\*)+?[\\w\\W]*?(\\*/)+))`)

	// join patterns
	joinCharsRegex = "[a-zA-Z0-9@_\\.\\`-]*"

	joinExpr = "(" +
		"DATE\\(" + joinCharsRegex + "\\)" +
		"|" +
		joinCharsRegex +
		")"
	joinOnTerminals = joinExpr + "\\s*\\=\\s*" + joinExpr

	joinRegex = "" +
		"(?i)(?:ON)\\s+" + joinOnTerminals + "(\\s+(AND|OR)\\s+" + joinOnTerminals + ")*" +
		"|" +
		"(?i)(?:USING)\\s*\\(\\s*([a-zA-Z0-9,@_\\s `-]*)\\s*\\)"
	joinPatterns = regexp.MustCompile(joinRegex)

	// filter patterns
	filterCharsRegex                    = "[a-zA-Z0-9@_\"\\',\\.\\x60-]*"
	filterCharsWithWhitespaceColonRegex = "[a-zA-Z0-9@_\\:\"\\',\\s\\.\\x60-]*"

	filterExprLHS = filterCharsRegex

	filterExprRHS = "(" +
		"CURRENT_TIMESTAMP\\(\\)" +
		"|" +
		"TIMESTAMP\\(" + filterCharsWithWhitespaceColonRegex + "\\)" +
		"|" +
		filterCharsRegex +
		")"

	filterTerminals = "(" +
		filterExprLHS + "\\s*(<=|>=|!=|<>|=|<|>)\\s*" + filterExprRHS +
		"|" +
		filterExprLHS + "\\s+(LIKE|NOT LIKE)\\s+" + filterExprRHS +
		"|" +
		filterExprLHS + "\\s+(BETWEEN|NOT BETWEEN)\\s+" + filterExprRHS + "\\s+AND\\s+" + filterExprRHS +
		"|" +
		filterExprLHS + "\\s+IS (?:NOT)?\\s?(S_NULL|TRUE|FALSE)" +
		"|" +
		filterExprLHS + "\\s+(IN|NOT IN)\\s?\\(" + filterCharsWithWhitespaceColonRegex + "\\)" +
		")"
	filterRegex    = "(?i)(?:WHERE|HAVING)\\s+" + filterTerminals + "(\\s+(AND|OR)\\s+" + filterTerminals + ")*"
	filterPatterns = regexp.MustCompile(filterRegex)
)

// ParseJoinConditions will return all join condition (ON and USING) in sql Query in a list of string
// where each of it is a join condition string
func ParseJoinConditions(sqlQuery string) []string {
	sqlQuery = cleanUpQuery(sqlQuery)

	var jcs []string
	matches := joinPatterns.FindAllStringSubmatch(sqlQuery, -1)
	for _, match := range matches {

		const joinConditionFullIdx = 0
		jcs = append(jcs, match[joinConditionFullIdx])
	}

	return jcs
}

// ParseFilterConditions will return all filter condition (WHERE and HAVING) in sql Query in a list of string
func ParseFilterConditions(sqlQuery string) []string {
	sqlQuery = cleanUpQuery(sqlQuery)

	return filterPatterns.FindAllString(sqlQuery, -1)
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
