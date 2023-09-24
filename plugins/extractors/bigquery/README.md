# bigquery

## Usage

```yaml
source:
  name: bigquery
  config:
    project_id: google-project-id
    table_pattern: gofood.fact_
    exclude:
      datasets:
        - dataset_a
        - dataset_b
      tables:
        - dataset_c.table_a
    max_page_size: 100
    profile_column: true
    build_view_lineage: true
    # Only one of service_account_base64 / service_account_json is needed. 
    # If both are present, service_account_base64 takes precedence
    service_account_base64: _________BASE64_ENCODED_SERVICE_ACCOUNT_________________
    service_account_json:
      {
        "type": "service_account",
        "private_key_id": "xxxxxxx",
        "private_key": "xxxxxxx",
        "client_email": "xxxxxxx",
        "client_id": "xxxxxxx",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "xxxxxxx",
        "client_x509_cert_url": "xxxxxxx"
      }
    collect_table_usage: false
    usage_period_in_day: 7
    usage_project_ids:
      - google-project-id
      - other-google-project-id
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :-- |
| `project_id` | `string` | `my-project` | BigQuery Project ID | *required* |
| `service_account_base64` | `string` | `____BASE64_ENCODED_SERVICE_ACCOUNT____` | Service Account in base64 encoded string. Takes precedence over `service_account_json` value | *optional* |
| `service_account_json` | `string` | `{"private_key": .., "private_id": ...}` | Service Account in JSON string | *optional* |
| `table_pattern` | `string` | `gofood.fact_` | Regex pattern to filter which bigquery table to scan (whitelist) | *optional* |
| `max_page_size` | `int` | `100` | max page size hint used for fetching datasets/tables/rows from bigquery | *optional* |
| `include_column_profile` | `bool` | `true` | true if you want to profile the column value such min, max, med, avg, top, and freq | *optional* |
| `max_preview_rows` | `int` | `30` | max number of preview rows to fetch, `0` will skip preview fetching. Default to `30`. | *optional* |
| `mix_values` | `bool` | `false` | true if you want to mix the column values with the preview rows. Default to `false`. | *optional* |
| `collect_table_usage` | `boolean` | `false` | toggle feature to collect table usage, `true` will enable collecting table usage. Default to `false`. | *optional* |
| `usage_period_in_day` | `int` | `7` | collecting log from `(now - usage_period_in_day)` until `now`. only matter if `collect_table_usage` is true. Default to `7`. | *optional* |
| `usage_project_ids` | `[]string` | `[google-project-id, other-google-project-id]` | collecting log from defined GCP Project IDs. Default to BigQuery Project ID. | *optional* |

### *Notes*

- Leaving `service_account_json` and `service_account_base64` blank will default
  to [Google's default authentication][google-default-auth]. It is
  recommended if Meteor instance runs inside the same Google Cloud environment as the BigQuery project.
- Service account needs to have `bigquery.privateLogsViewer` role to be able to collect bigquery audit logs

## Outputs

| Field                          | Sample Value                                                                                                                                                                                                                                                                                                                                                                                | Description                                             |
|:-------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------|
| `resource.urn`                 | `project_id.dataset_name.table_name`                                                                                                                                                                                                                                                                                                                                                        |                                                         |
| `resource.name`                | `table_name`                                                                                                                                                                                                                                                                                                                                                                                |                                                         |
| `resource.service`             | `bigquery`                                                                                                                                                                                                                                                                                                                                                                                  |                                                         |
| `description`                  | `table description`                                                                                                                                                                                                                                                                                                                                                                         |                                                         |
| `profile.total_rows`           | `2100`                                                                                                                                                                                                                                                                                                                                                                                      |                                                         |
| `profile.usage_count`          | `15`                                                                                                                                                                                                                                                                                                                                                                                        |                                                         |
| `profile.joins`                | [][Join](#Join)                                                                                                                                                                                                                                                                                                                                                                             |                                                         |
| `profile.filters`              | [`"WHERE t.param_3 = 'the_param' AND t.column_1 = \"xxxxxx-xxxx-xxxx-xxxx-xxxxxxxxx\""`,`"WHERE event_timestamp >= TIMESTAMP(\"2021-10-29\", \"UTC\") AND event_timestamp < TIMESTAMP(\"2021-11-22T02:01:06Z\")"`]                                                                                                                                                                          |                                                         |
| `schema`                       | [][Column](#column)                                                                                                                                                                                                                                                                                                                                                                         |                                                         |
| `properties.partition_data`    | `"partition_data": {"partition_field": "data_date", "require_partition_filter": false, "time_partition": {"partition_by": "DAY","partition_expire": 0 } }`                                                                                                                                                                                                                                  | partition related data for time and range partitioning. |
| `properties.clustering_fields` | `['created_at', 'updated_at']`                                                                                                                                                                                                                                                                                                                                                              | list of fields on which the table is clustered          |
| `properties.partition_field`   | `created_at`                                                                                                                                                                                                                                                                                                                                                                                | returns the field on which table is time partitioned    |

### Partition Data

| Field                                     | Sample Value | Description                                                                                                                                                                                                                                                             |
|:------------------------------------------|:-------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `partition_field`                         | `created_at` | field on which the table is partitioned either by TimePartitioning or RangePartitioning. In case field is empty for TimePartitioning _PARTITIONTIME is returned instead of empty.                                                                                       |
| `require_partition_filter`                | `true`       | boolean value which denotes if every query on the bigquery table must include at least one predicate that only references the partitioning column                                                                                                                       |
| `time_partition.partition_by`             | `HOUR`       | returns partition type HOUR/DAY/MONTH/YEAR                                                                                                                                                                                                                              |
| `time_partition.partition_expire_seconds` | `0`          | time in which data will expire from this partition. If 0 it will not expire.                                                                                                                                                                                            |
| `range_partition.interval`                | `10`         | width of a interval range                                                                                                                                                                                                                                               |
| `range_partition.start`                   | `0`          | start value for partition inclusive of this value                                                                                                                                                                                                                       |
| `range_partition.end`                     | `100`        | end value for partition exclusive of this value                                                                                                                                                                                                                         |


### Column

| Field         | Sample Value                           |
|:--------------|:---------------------------------------|
| `name`        | `total_price`                          |
| `description` | `item's total price`                   |
| `data_type`   | `decimal`                              |
| `is_nullable` | `true`                                 |
| `length`      | `12,2`                                 |
| `profile`     | `{"min":...,"max": ...,"unique": ...}` |

### Join

| Field        | Sample Value                                                                                                                                            |
|:-------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `urn`        | `project_id.dataset_name.table_name`                                                                                                                    |
| `count`      | `3`                                                                                                                                                     |
| `conditions` | [`"ON target.column_1 = source.column_1 and target.param_name = source.param_name"`,`"ON DATE(target.event_timestamp) = DATE(source.event_timestamp)"`] |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on 
contributing to this module.

[google-default-auth]: https://cloud.google.com/docs/authentication/production#automatically
