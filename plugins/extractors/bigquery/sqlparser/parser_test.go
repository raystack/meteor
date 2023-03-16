//go:build plugins
// +build plugins

package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSimpleJoin(t *testing.T) {

	type set map[string]bool
	newSet := func(values ...string) set {
		s := make(set)
		for _, val := range values {
			s[val] = true
		}
		return s
	}
	testCases := []struct {
		Name     string
		Query    string
		JoinInfo set
	}{
		{
			Name:     "simple query with simple join with on",
			Query:    "select * from data-engineering.testing.table1 join data-engineering.testing.table2 on some_field1 = some_field2",
			JoinInfo: newSet("on some_field1 = some_field2"),
		},
		{
			Name:     "simple query with simple join with on  and unformatted",
			Query:    "select * from data-engineering.testing.table1 join data-engineering.testing.table2 on some_field1 =some_field2",
			JoinInfo: newSet("on some_field1 =some_field2"),
		},
		{
			Name:     "simple query with simple join with using ",
			Query:    "select * from data-engineering.testing.table1 join data-engineering.testing.table2 using (some_field)",
			JoinInfo: newSet("using (some_field)"),
		},
		{
			Name:     "simple query with simple join with using and multiple columns",
			Query:    "select * from data-engineering.testing.table1 join data-engineering.testing.table2 using (some_field, some_field1, somefield3)",
			JoinInfo: newSet("using (some_field, some_field1, somefield3)"),
		},
		{
			Name:     "simple query with simple join with using and multiple columns and unformatted",
			Query:    "select * from data-engineering.testing.table1 join data-engineering.testing.table2 using (some_field, some_field1,somefield3)",
			JoinInfo: newSet("using (some_field, some_field1,somefield3)"),
		},
		{
			Name:     "simple query with outer join and `on`",
			Query:    "select * from data-engineering.testing.table1 full outer join data-engineering.testing.table2 on some_field1 = some_field2",
			JoinInfo: newSet("on some_field1 = some_field2"),
		},
		{
			Name:     "simple query with outer join and `using`",
			Query:    "select * from data-engineering.testing.table1 full outer join data-engineering.testing.table2 using (some_field1, some_field2)",
			JoinInfo: newSet("using (some_field1, some_field2)"),
		},
		{
			Name:     "subquery and join",
			Query:    "select * from (select order_id from data-engineering.testing.orders) join data-engineering.testing.table2 using (some_field1, some_field2)",
			JoinInfo: newSet("using (some_field1, some_field2)"),
		},
		{
			Name:     "`with` clause + join",
			Query:    "with dedup_source as (select * from `project.fire.fly`) select * from dedup_source join `project.maximum.overdrive` on dedup_source.left = `project.maximum.overdrive`.right",
			JoinInfo: newSet("on dedup_source.left = `project.maximum.overdrive`.right"),
		},
		{
			Name: "more than 2 joins",
			Query: `SELECT
					t1.field1 AS field1,
					t2.field2 AS field2,
					t1.field3 AS field3,
					t3.field4 AS field4` +
				"FROM `project1.dataset1.table1` t1" +
				"JOIN `project2.dataset1.table1` t2 ON t1.somefield = t2.anotherfield " +
				"JOIN `project3.dataset1.table1` t3 ON t1.somefield = t3.yetanotherfield",
			JoinInfo: newSet("ON t1.somefield = t2.anotherfield", "ON t1.somefield = t3.yetanotherfield"),
		},
	}

	for _, test := range testCases {
		t.Run(test.Name, func(t *testing.T) {
			jcs := ParseJoinConditions(test.Query)

			assert.Equal(t, test.JoinInfo, newSet(jcs...))
		})
	}
}

func TestParseSimpleFilter(t *testing.T) {

	type set map[string]bool
	newSet := func(values ...string) set {
		s := make(set)
		for _, val := range values {
			s[val] = true
		}
		return s
	}
	testCases := []struct {
		Name            string
		Query           string
		FilterCondition set
	}{
		{
			Name:            "simple query with where in the middle and order",
			Query:           "select start_time from `region-us`.information_schema.jobs_by_user where job_type=\"query\" and statement_type=\"insert\" order by start_time desc limit 1",
			FilterCondition: newSet("where job_type=\"query\" and statement_type=\"insert\""),
		},
		{
			Name:            "simple query with where in the middle and limit",
			Query:           "select start_time from `region-us`.information_schema.jobs_by_user where job_type=\"query\" and statement_type=\"insert\" limit 1",
			FilterCondition: newSet("where job_type=\"query\" and statement_type=\"insert\""),
		},
		{
			Name:            "`with` clause and where in subquery",
			Query:           "with subq1 as (select * from roster where schoolid = 52 limit 1), subq2 as (select schoolid from subq1) select distinct * from subq2;",
			FilterCondition: newSet("where schoolid = 52"),
		},
		{
			Name:            "`with` clause and where in subquery with sequence of bool expression",
			Query:           "with subq1 as (select * from roster where schoolid = 52 and param=\"a_string\" and param2= 4), subq2 as (select schoolid from subq1) select distinct * from subq2;",
			FilterCondition: newSet("where schoolid = 52 and param=\"a_string\" and param2= 4"),
		},
		{
			Name:            "`with` clause and where in multiple subquery with sequence of bool expression",
			Query:           "with subq1 as (select * from roster where schoolid = 52 and param=\"a_string\"), subq2 as (select schoolid from subq1 where schoolid = 13 and param=\"is_a_string\") select distinct * from subq2;",
			FilterCondition: newSet("where schoolid = 52 and param=\"a_string\"", "where schoolid = 13 and param=\"is_a_string\""),
		},
		{
			Name:            "simple query with simple join with using and multiple columns and where with sequence of bool expression",
			Query:           "select * from data-engineering.testing.table1 join data-engineering.testing.table2 using (some_field, some_field1, somefield3) where job_type=\"query\" and statement_type=\"insert\" limit 1",
			FilterCondition: newSet("where job_type=\"query\" and statement_type=\"insert\""),
		},
		{
			Name:            "simple select with where timestamp function",
			Query:           "SELECT * FROM `dataset-1-name.bq-dataset-all` WHERE event_timestamp between TIMESTAMP('2021-11-20')  AND TIMESTAMP('2021-11-21')",
			FilterCondition: newSet("WHERE event_timestamp between TIMESTAMP('2021-11-20') AND TIMESTAMP('2021-11-21')"),
		},
		{
			Name: "complex query with comment",
			Query: `SELECT
					COUNT(DISTINCT user_id) AS purchasers_count
					FROM
					-- PLEASE REPLACE WITH YOUR TABLE NAME.
					` + "`" + "YOUR_TABLE.events_*" + "`" + `
					WHERE
					event_name IN ('in_app_purchase', 'purchase')
					-- PLEASE REPLACE WITH YOUR DESIRED DATE RANGE
					AND _TABLE_SUFFIX BETWEEN '20180501' AND '20240131'
					AND ` + "`" + "_TABLE_SUFFIX" + "`" + ` BETWEEN '1' AND '2';`,
			FilterCondition: newSet("WHERE event_name IN ('in_app_purchase', 'purchase') AND _TABLE_SUFFIX BETWEEN '20180501' AND '20240131' AND `_TABLE_SUFFIX` BETWEEN '1' AND '2'"),
		},
	}

	for _, test := range testCases {
		t.Run(test.Name, func(t *testing.T) {
			fcs := ParseFilterConditions(test.Query)
			assert.Equal(t, test.FilterCondition, newSet(fcs...))
		})
	}
}

func TestComplexQueries(t *testing.T) {
	type set map[string]bool
	newSet := func(values ...string) set {
		s := make(set)
		for _, val := range values {
			s[val] = true
		}
		return s
	}
	testCases := []struct {
		Name            string
		Query           string
		JoinInfo        set
		FilterCondition set
	}{
		{
			Name:            "complex query 1",
			Query:           testDataSQL1,
			JoinInfo:        newSet(),
			FilterCondition: newSet("WHERE event_name IN ('in_app_purchase', 'purchase') AND _TABLE_SUFFIX BETWEEN '20180501' AND '20240131' AND `_TABLE_SUFFIX` BETWEEN '1' AND '2'"),
		},
		{
			Name:            "complex query 2",
			Query:           testDataSQL2,
			JoinInfo:        newSet("ON target.column_1 = source.column_1 and target.variant_name = source.variant_name and DATE(target.event_timestamp) = DATE(source.event_timestamp)"),
			FilterCondition: newSet("WHERE t.column_type = 'tester' AND t.param_id = \"280481a2-2384-4b81-aa3e-214ac60b31db\" AND event_timestamp >= TIMESTAMP(\"2021-10-29\", \"UTC\") AND event_timestamp < TIMESTAMP(\"2021-11-22T02:01:06Z\")"),
		},
		{
			Name:            "complex query 3",
			Query:           testDataSQL3,
			JoinInfo:        newSet(),
			FilterCondition: newSet("WHERE traffic_source.source = 'google' AND traffic_source.medium = 'cpc' AND traffic_source.name = 'VTA-Test-Android' AND _TABLE_SUFFIX BETWEEN '20180521' AND '20240131'"),
		},
	}

	for _, test := range testCases {
		t.Run(test.Name, func(t *testing.T) {
			jcs := ParseJoinConditions(test.Query)
			assert.Equal(t, test.JoinInfo, newSet(jcs...))

			fcs := ParseFilterConditions(test.Query)
			assert.Equal(t, test.FilterCondition, newSet(fcs...))
		})
	}
}

const testDataSQL1 = `
SELECT
COUNT(DISTINCT user_id) AS purchasers_count
FROM
-- PLEASE REPLACE WITH YOUR TABLE NAME.
` + "`" + "YOUR_TABLE.events_*" + "`" + `
WHERE
event_name IN ('in_app_purchase', 'purchase')
-- PLEASE REPLACE WITH YOUR DESIRED DATE RANGE
AND _TABLE_SUFFIX BETWEEN '20180501' AND '20240131'
AND ` + "`" + "_TABLE_SUFFIX" + "`" + ` BETWEEN '1' AND '2';`

const testDataSQL2 = `
USING (
    SELECT 
    t.column_1,
    t.column_2,
    CAST(count(distinct(id)) AS BIGNUMERIC) as total,
    TIMESTAMP("2021-10-29", "UTC") as window_start_time,
    CAST("2021-11-22T02:01:06Z" AS TIMESTAMP) as event_timestamp
    FROM` + "`" + `a-dataset.a-specific_table-2021` + "`" + `, 
    UNNEST(trial) as t 
    WHERE 
        t.column_type = 'tester' AND
        t.param_id = "280481a2-2384-4b81-aa3e-214ac60b31db" AND 
        event_timestamp >= TIMESTAMP("2021-10-29", "UTC") AND
        event_timestamp < TIMESTAMP("2021-11-22T02:01:06Z")
    GROUP BY t.column_1, t.column_2
    ) source 
    ON
        target.column_1 = source.column_1
        and target.variant_name = source.variant_name
        and DATE(target.event_timestamp) = DATE(source.event_timestamp)
    WHEN matched then update set 
        target.total = source.total,
        target.event_timestamp = source.event_timestamp
    WHEN NOT MATCHED then insert(param1,param2,param3,param4,param5_ext) values(
        source.param1,
        source.param2,
        source.param3,
        source.param4,
        source.param5_ext
    )`

const testDataSQL3 = `
SELECT
  COUNT(DISTINCT user_id) AS acquired_users_count
FROM
  -- PLEASE REPLACE WITH YOUR TABLE NAME.
  ` + "`" + "YOUR_TABLE.events_*" + "`" + `
WHERE
  traffic_source.source = 'google'
  AND traffic_source.medium = 'cpc'
  AND traffic_source.name = 'VTA-Test-Android'
  -- PLEASE REPLACE YOUR DESIRED DATE RANGE.
  AND _TABLE_SUFFIX BETWEEN '20180521' AND '20240131';`
