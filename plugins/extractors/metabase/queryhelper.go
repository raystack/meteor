package metabase

import (
	"fmt"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

func evaluateQueryTemplate(datasetQuery NativeDatasetQuery) (string, error) {
	query := datasetQuery.Query

	// clean metabase custom syntax
	query = strings.ReplaceAll(query, "[[", "")
	query = strings.ReplaceAll(query, "]]", "")

	if datasetQuery.TemplateTags == nil {
		return "", nil
	}
	for key, tag := range datasetQuery.TemplateTags {
		templateValue, err := getTemplateDefaultValue(tag.Type)
		if err != nil {
			return "", fmt.Errorf("build template default value: %w", err)
		}

		query = strings.ReplaceAll(
			query,
			fmt.Sprintf("{{%s}}", key),
			templateValue,
		)
	}

	return query, nil
}

func getTemplateDefaultValue(tempType string) (value string, err error) {
	switch tempType {
	case "date":
		return "CURRENT_DATE()", nil

	case "string", "text":
		return "sample-text", nil

	case "number":
		return "0", nil

	case "dimension":
		return "1", nil

	default:
		return "", fmt.Errorf("unsupported template type %q", tempType)
	}
}

func extractTableNamesFromSQL(query string) ([]string, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("parse SQL: %w", err)
	}

	names, err := parseSelectStatement(stmt)
	if err != nil {
		return nil, fmt.Errorf("parse select statement: %w", err)
	}

	return names, nil
}

func parseSelectStatement(selectStmt sqlparser.Statement) ([]string, error) {
	switch stmt := selectStmt.(type) {
	case *sqlparser.Select:
		var tableNames []string
		for _, from := range stmt.From {
			names, err := parseTableExpr(from)
			if err != nil {
				return nil, err
			}

			tableNames = append(tableNames, names...)
		}

		return tableNames, nil

	case *sqlparser.Union:
		leftNames, err := parseSelectStatement(stmt.Left)
		if err != nil {
			return nil, err
		}

		rightNames, err := parseSelectStatement(stmt.Right)
		if err != nil {
			return nil, err
		}

		return append(leftNames, rightNames...), nil

	case *sqlparser.ParenSelect:
		return parseSelectStatement(stmt.Select)

	default:
		return nil, fmt.Errorf("unhandled Statement type \"%T\"", stmt)
	}
}

func parseTableExpr(tableExpr sqlparser.TableExpr) ([]string, error) {
	switch expr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return parseSimpleTableExpr(expr.Expr)

	case *sqlparser.JoinTableExpr:
		leftNames, err := parseTableExpr(expr.LeftExpr)
		if err != nil {
			return nil, err
		}

		rightNames, err := parseTableExpr(expr.RightExpr)
		if err != nil {
			return nil, err
		}
		return append(leftNames, rightNames...), nil

	default:
		return nil, fmt.Errorf("unhandled TableExpr type \"%T\"", expr)
	}
}

func parseSimpleTableExpr(simpleTableExpr sqlparser.SimpleTableExpr) ([]string, error) {
	switch expr := simpleTableExpr.(type) {
	case sqlparser.TableName:
		name := expr.Name.String()
		if expr.Qualifier.String() != "" {
			name = expr.Qualifier.String() + "." + name
		}

		return []string{name}, nil

	case *sqlparser.Subquery:
		return parseSelectStatement(expr.Select)

	default:
		return nil, fmt.Errorf("unhandled SimpleTableExpr type \"%T\"", expr)
	}
}
