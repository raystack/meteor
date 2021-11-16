package sqlparser

// queries from this article
// https://support.google.com/analytics/answer/9037342?hl=en#zippy=%2Cin-this-article

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

// we haven't supported the `WHERE ... function()`
// const testDataSQL2 = `
// SELECT
//   COUNT(DISTINCT user_id) AS n_day_active_users_count
// FROM
//   -- PLEASE REPLACE WITH YOUR TABLE NAME.
//   ` + "`" + "YOUR_TABLE.events_*" + "`" + ` AS T
//     CROSS JOIN
//       T.event_params
// WHERE
//   event_params.key = 'engagement_time_msec' AND event_params.value.int_value > 0
//   -- Pick events in the last N = 20 days.
//   AND event_timestamp >
//       UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 20 DAY))
//   -- PLEASE REPLACE WITH YOUR DESIRED DATE RANGE.
//   AND _TABLE_SUFFIX BETWEEN '20180521' AND '20240131';`

// const testDataSQL3 = `
// SELECT
//   COUNT(DISTINCT MDaysUsers.user_id) AS n_day_inactive_users_count
// FROM
//   (
//     SELECT
//       user_id
//     FROM
//       /* PLEASE REPLACE WITH YOUR TABLE NAME */
//       ` + "`" + "YOUR_TABLE.events_*" + "`" + ` AS T
//     CROSS JOIN
//       T.event_params
//     WHERE
//       event_params.key = 'engagement_time_msec' AND event_params.value.int_value > 0
//       /* Has engaged in last M = 7 days */
//       AND event_timestamp >
//           UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
//       /* PLEASE REPLACE WITH YOUR DESIRED DATE RANGE */
//       AND _TABLE_SUFFIX BETWEEN '20180521' AND '20240131'
//   ) AS MDaysUsers
// -- EXCEPT ALL is not yet implemented in BigQuery. Use LEFT JOIN in the interim.
// LEFT JOIN
//   (
//     SELECT
//       user_id
//     FROM
//       /* PLEASE REPLACE WITH YOUR TABLE NAME */
//       ` + "`" + "YOUR_TABLE.events_*" + "`" + `AS T
//     CROSS JOIN
//       T.event_params
//     WHERE
//       event_params.key = 'engagement_time_msec' AND event_params.value.int_value > 0
//       /* Has engaged in last N = 2 days */
//       AND event_timestamp >
//           UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))
//       /* PLEASE REPLACE WITH YOUR DESIRED DATE RANGE */
//       AND _TABLE_SUFFIX BETWEEN '20180521' AND '20240131'
//   ) AS NDaysUsers
//   ON MDaysUsers.user_id = NDaysUsers.user_id
// WHERE
//   NDaysUsers.user_id IS NULL;`

// const testDataSQL4 = `
// SELECT
//   COUNT(DISTINCT user_id) AS frequent_active_users_count
// FROM
//   (
//     SELECT
//       user_id,
//       COUNT(DISTINCT event_date)
//     FROM
//       -- PLEASE REPLACE WITH YOUR TABLE NAME.
//       ` + "`" + "YOUR_TABLE.events_*" + "`" + ` AS T
//     CROSS JOIN
//       T.event_params
//     WHERE
//       event_params.key = 'engagement_time_msec' AND event_params.value.int_value > 0
//       -- User engagement in the last M = 10 days.
//       AND event_timestamp >
//           UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY))
//       -- PLEASE REPLACE YOUR DESIRED DATE RANGE.  For optimal performance
//       -- the _TABLE_SUFFIX range should match the INTERVAL value above.
//       AND _TABLE_SUFFIX BETWEEN '20180521' AND '20240131'
//     GROUP BY 1
//     -- Having engaged in at least N = 4 days.
//     HAVING COUNT(event_date) >= 4
//   );`

// const testDataSQL5 = `
// SELECT
//   COUNT(DISTINCT user_id) AS high_active_users_count
// FROM
//   (
//     SELECT
//       user_id,
//       event_params.key,
//       SUM(event_params.value.int_value)
//     FROM
//       -- PLEASE REPLACE WITH YOUR TABLE NAME.
//       ` + "`" + "YOUR_TABLE.events_*" + "`" + ` AS T
//     CROSS JOIN
//       T.event_params
//     WHERE
//       -- User engagement in the last M = 10 days.
//       event_timestamp >
//           UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY))
//       AND event_params.key = 'engagement_time_msec'
//       -- PLEASE REPLACE YOUR DESIRED DATE RANGE.
//       AND _TABLE_SUFFIX BETWEEN '20180521' AND '20240131'
//     GROUP BY 1, 2
//     HAVING
//       -- Having engaged for more than N = 0.1 minutes.
//       SUM(event_params.value.int_value) > 0.1 * 60 * 1000000
//   );`

const testDataSQL6 = `
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

// const testDataSQL7 = `
// SELECT
//   COUNT(DISTINCT user_id) AS users_acquired_through_google_count
// FROM
//   -- PLEASE REPLACE WITH YOUR TABLE NAME.
//   ` + "`" + "YOUR_TABLE.events_*" + "`" + `
// WHERE
//   event_name = 'first_open'
//   -- Cohort: opened app 1-2 weeks ago. One week of cohort, aka. weekly.
//   AND event_timestamp >
//       UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY))
//   AND event_timestamp <
//       UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
//   -- Cohort filter: users acquired through 'google' source.
//   AND traffic_source.source = 'google'
//   -- PLEASE REPLACE YOUR DESIRED DATE RANGE.
//   AND _TABLE_SUFFIX BETWEEN '20180501' AND '20240131';`
