package sqlparser

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
