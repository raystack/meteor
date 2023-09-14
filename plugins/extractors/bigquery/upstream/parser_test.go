package upstream_test

import (
	"testing"

	"github.com/goto/meteor/plugins/extractors/bigquery/upstream"
	"github.com/stretchr/testify/assert"
)

func TestParseTopLevelUpstreamsFromQuery(t *testing.T) {
	t.Run("parse test", func(t *testing.T) {
		type _set map[upstream.Resource]bool
		newSetFn := func(resources ...upstream.Resource) _set {
			set := make(_set)

			for _, r := range resources {
				set[r] = true
			}

			return set
		}

		testCases := []struct {
			Name            string
			InputQuery      string
			ExpectedSources []upstream.Resource
		}{
			{
				Name:       "simple query",
				InputQuery: "select * from data-engineering.testing.table1",
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table1",
					},
				},
			},
			{
				Name:       "simple query with hyphenated table name",
				InputQuery: "select * from data-engineering.testing.table_name-1",
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table_name-1",
					},
				},
			},
			{
				Name:       "simple query with quotes",
				InputQuery: "select * from `data-engineering.testing.table1`",
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table1",
					},
				},
			},
			{
				Name:            "simple query without project name",
				InputQuery:      "select * from testing.table1",
				ExpectedSources: []upstream.Resource{},
			},
			{
				Name:       "simple query with simple join",
				InputQuery: "select * from data-engineering.testing.table1 join data-engineering.testing.table2 on some_field",
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table1",
					},
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table2",
					},
				},
			},
			{
				Name:       "simple query with outer join",
				InputQuery: "select * from data-engineering.testing.table1 outer join data-engineering.testing.table2 on some_field",
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table1",
					},
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table2",
					},
				},
			},
			{
				Name:       "subquery",
				InputQuery: "select * from (select order_id from data-engineering.testing.orders)",
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "orders",
					},
				},
			},
			{
				Name:       "`with` clause + simple query",
				InputQuery: "with `information.foo.bar` as (select * from `data-engineering.testing.data`) select * from `information.foo.bar`",
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "data",
					},
				},
			},
			{
				Name:       "`with` clause with missing project name",
				InputQuery: "with `foo.bar` as (select * from `data-engineering.testing.data`) select * from `foo.bar`",
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "data",
					},
				},
			},
			{
				Name:       "project name with dashes",
				InputQuery: "select * from `foo-bar.baz.data`",
				ExpectedSources: []upstream.Resource{
					{
						Project: "foo-bar",
						Dataset: "baz",
						Name:    "data",
					},
				},
			},
			{
				Name:       "dataset and project name with dashes",
				InputQuery: "select * from `foo-bar.bar-baz.data",
				ExpectedSources: []upstream.Resource{
					{
						Project: "foo-bar",
						Dataset: "bar-baz",
						Name:    "data",
					},
				},
			},
			{
				Name:       "`with` clause + join",
				InputQuery: "with dedup_source as (select * from `project.fire.fly`) select * from dedup_source join `project.maximum.overdrive` on dedup_source.left = `project.maximum.overdrive`.right",
				ExpectedSources: []upstream.Resource{
					{
						Project: "project",
						Dataset: "fire",
						Name:    "fly",
					},
					{
						Project: "project",
						Dataset: "maximum",
						Name:    "overdrive",
					},
					{
						Project: "project",
						Dataset: "maximum",
						Name:    "overdrive",
					},
				},
			},
			{
				Name:       "double `with` + pseudoreference",
				InputQuery: "with s1 as (select * from internal.pseudo.ref), with internal.pseudo.ref as (select * from `project.another.name`) select * from s1",
				ExpectedSources: []upstream.Resource{
					{
						Project: "project",
						Dataset: "another",
						Name:    "name",
					},
				},
			},
			{
				Name:            "simple query that ignores from upstream",
				InputQuery:      "select * from /* @ignoreupstream */ data-engineering.testing.table1",
				ExpectedSources: []upstream.Resource{},
			},
			{
				Name:            "simple query that ignores from upstream with quotes",
				InputQuery:      "select * from /* @ignoreupstream */ `data-engineering.testing.table1`",
				ExpectedSources: []upstream.Resource{},
			},
			{
				Name:       "simple query with simple join that ignores from upstream",
				InputQuery: "select * from /* @ignoreupstream */ data-engineering.testing.table1 join data-engineering.testing.table2 on some_field",
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table2",
					},
				},
			},
			{
				Name:       "simple query with simple join that has comments but does not ignores upstream",
				InputQuery: "select * from /*  */ data-engineering.testing.table1 join data-engineering.testing.table2 on some_field",
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table1",
					},
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table2",
					},
				},
			},
			{
				Name:       "simple query with simple join that ignores upstream of join",
				InputQuery: "select * from data-engineering.testing.table1 join /* @ignoreupstream */ data-engineering.testing.table2 on some_field",
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table1",
					},
				},
			},
			{
				Name: "simple query with an ignoreupstream for an alias should still consider it as dependency",
				InputQuery: `
					WITH my_temp_table AS (
						SELECT id, name FROM data-engineering.testing.an_upstream_table
					)
					SELECT id FROM /* @ignoreupstream */ my_temp_table
					`,
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "an_upstream_table",
					},
				},
			},
			{
				Name: "simple query should have alias in the actual name rather than with alias",
				InputQuery: `
					WITH my_temp_table AS (
						SELECT id, name FROM /* @ignoreupstream */ data-engineering.testing.an_upstream_table
					)
					SELECT id FROM my_temp_table
					`,
				ExpectedSources: []upstream.Resource{},
			},
			{
				Name:            "simple query with simple join that ignores upstream of join",
				InputQuery:      "WITH my_temp_table AS ( SELECT id, name FROM /* @ignoreupstream */ data-engineering.testing.an_upstream_table ) SELECT id FROM /* @ignoreupstream */ my_temp_table",
				ExpectedSources: []upstream.Resource{},
			},
			{
				Name: "simple query with another query inside comment",
				InputQuery: `
					select * from data-engineering.testing.tableABC
					-- select * from data-engineering.testing.table1 join data-engineering.testing.table2 on some_field
					`,
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "tableABC",
					},
				},
			},
			{
				Name: "query with another query inside comment and a join that uses helper",
				InputQuery: `
					select * from data-engineering.testing.tableABC
					/* select * from data-engineering.testing.table1 join data-engineering.testing.table2 on some_field */
					join /* @ignoreupstream */ data-engineering.testing.table2 on some_field
					`,
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "tableABC",
					},
				},
			},
			{
				Name: "ignore `create view` in ddl query",
				InputQuery: `
					create view data-engineering.testing.tableABC
					select *
					from
						data-engineering.testing.tableDEF,
					`,
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "tableDEF",
					},
				},
			},
			{
				Name: "one or more sources are stated together under from clauses",
				InputQuery: `
					select *
					from
						pseudo_table1,
						` + "`data-engineering.testing.tableABC`," + `
						pseudo_table2 as pt2
						` + "`data-engineering.testing.tableDEF`," + ` as backup_table,
						/* @ignoreupstream */ data-engineering.testing.tableGHI as ignored_table,
					`,
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "tableABC",
					},
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "tableDEF",
					},
				},
			},
			{
				Name: "one or more sources are from wild-card query",
				InputQuery: `
					select *
					from data-engineering.testing.tableA*

					select *
					from ` +
					"`data-engineering.testing.tableB*`" + `

					select *
					from
						/*@ignoreupstream*/ data-engineering.testing.tableC*
					`,
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "tableA*",
					},
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "tableB*",
					},
				},
			},
			{
				Name: "ignore characters after -- comment",
				InputQuery: `
				-- sources
				-- data-engineering.testing.table_a
				--
				-- related
				-- ` + "`data-engineering.testing.table_b`" + `
				-- from data-engineering.testing.table_c

				select *
				from data-engineering.testing.table_a
				join /* @ignoreupstream */ data-engineering.testing.table_d
				`,
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table_a",
					},
				},
			},
			{
				Name: "ignore characters within multi-line comment /* (separate line) */",
				InputQuery: `
				/*
				this the following relates to this table:

					with ` + "`data-engineering.testing.tabel_b`" + `
					from data-engineering.testing.tabel_c
				*/


				select *
				from
					data-engineering.testing.table_a
				join
					data-engineering.testing.table_d
				join
					/* @ignoreupstream */ data-engineering.testing.table_e
				`,
				ExpectedSources: []upstream.Resource{
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table_a",
					},
					{
						Project: "data-engineering",
						Dataset: "testing",
						Name:    "table_d",
					},
				},
			},
		}

		for _, test := range testCases {
			t.Run(test.Name, func(t *testing.T) {
				actualSources := upstream.ParseTopLevelUpstreamsFromQuery(test.InputQuery)

				actualSet := newSetFn(actualSources...)
				expectedSet := newSetFn(test.ExpectedSources...)

				assert.Equal(t, expectedSet, actualSet)
			})
		}
	})
}
