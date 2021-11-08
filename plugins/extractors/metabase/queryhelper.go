package metabase

import (
	"fmt"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/pkg/errors"
)

func evaluateQueryTemplate(datasetQuery NativeDatasetQuery) (query string, err error) {
	query = datasetQuery.Query

	// clean metabase custom syntax
	query = strings.ReplaceAll(query, "[[", "")
	query = strings.ReplaceAll(query, "]]", "")

	if datasetQuery.TemplateTags == nil {
		return
	}
	for key, tag := range datasetQuery.TemplateTags {
		templateValue, err := getTemplateDefaultValue(tag.Type)
		if err != nil {
			return query, errors.Wrapf(err, "error building template default value")
		}

		query = strings.ReplaceAll(
			query,
			fmt.Sprintf("{{%s}}", key),
			templateValue,
		)
	}

	return
}

func getTemplateDefaultValue(tempType string) (value string, err error) {
	switch tempType {
	case "date":
		value = "CURRENT_DATE()"
	case "string":
		fallthrough
	case "text":
		value = "sample-text"
	case "number":
		value = "0"
	case "dimension":
		value = "1"
	default:
		err = fmt.Errorf("unsupported template type \"%s\"", tempType)
	}

	return
}

func extractTableNamesFromSQL(query string) (tableNames []string, err error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		err = errors.Wrap(err, "error when parsing SQL")
		return
	}

	names, err := parseSelectStatement(stmt)
	if err != nil {
		err = errors.Wrap(err, "error parsing select statement")
		return
	}
	tableNames = append(tableNames, names...)

	return
}

func parseSelectStatement(selectStmt sqlparser.Statement) (tableNames []string, err error) {
	var names []string

	switch stmt := selectStmt.(type) {
	case *sqlparser.Select:
		for _, from := range stmt.From {
			names, err = parseTableExpr(from)
			if err != nil {
				return
			}
			tableNames = append(tableNames, names...)
		}
	case *sqlparser.Union:
		names, err = parseSelectStatement(stmt.Left)
		if err != nil {
			return
		}
		tableNames = append(tableNames, names...)

		names, err = parseSelectStatement(stmt.Right)
		if err != nil {
			return
		}
		tableNames = append(tableNames, names...)
	case *sqlparser.ParenSelect:
		names, err = parseSelectStatement(stmt.Select)
		if err != nil {
			return
		}
		tableNames = append(tableNames, names...)
	default:
		err = fmt.Errorf("unhandled Statement type \"%T\"", stmt)
	}

	return
}

func parseTableExpr(tableExpr sqlparser.TableExpr) (tableNames []string, err error) {
	var names []string

	switch expr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		names, err = parseSimpleTableExpr(expr.Expr)
		if err != nil {
			return
		}
		tableNames = append(tableNames, names...)
	case *sqlparser.JoinTableExpr:
		names, err = parseTableExpr(expr.LeftExpr)
		if err != nil {
			return
		}
		tableNames = append(tableNames, names...)

		names, err = parseTableExpr(expr.RightExpr)
		if err != nil {
			return
		}
		tableNames = append(tableNames, names...)
	default:
		err = fmt.Errorf("unhandled TableExpr type \"%T\"", expr)
	}

	return
}

func parseSimpleTableExpr(simpleTableExpr sqlparser.SimpleTableExpr) (tableNames []string, err error) {
	var names []string

	switch expr := simpleTableExpr.(type) {
	case sqlparser.TableName:
		name := expr.Name.String()
		if expr.Qualifier.String() != "" {
			name = expr.Qualifier.String() + "." + name
		}
		tableNames = append(tableNames, name)
	case *sqlparser.Subquery:
		names, err = parseSelectStatement(expr.Select)
		if err != nil {
			return
		}
		tableNames = append(tableNames, names...)
	default:
		err = fmt.Errorf("unhandled SimpleTableExpr type \"%T\"", expr)
	}

	return
}
