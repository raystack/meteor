# BigQuery

Extract table metadata, schema, statistics, and lineage from Google BigQuery.

## Usage

```yaml
source:
  name: bigquery
  config:
    project_id: google-project-id
    table_pattern: gofood.fact_
    max_preview_rows: 3
    exclude:
      datasets:
        - dataset_a
        - dataset_b
      tables:
        - dataset_c.table_a
    max_page_size: 100
    include_column_profile: true
    build_view_lineage: true
    # Only one of service_account_base64 / service_account_json is needed.
    # If both are present, service_account_base64 takes precedence.
    service_account_base64: ____base64_encoded_service_account____
    service_account_json: |-
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
    concurrency: 10
```

## Configuration

| Key | Value | Example | Description | |
| :-- | :---- | :------ | :---------- | :-- |
| `project_id` | `string` | `my-project` | BigQuery Project ID | _required_ |
| `service_account_base64` | `string` | `____BASE64____` | Base64-encoded service account JSON. Takes precedence over `service_account_json` | _optional_ |
| `service_account_json` | `string` | `{"private_key": ...}` | Service account credentials as a JSON string | _optional_ |
| `table_pattern` | `string` | `gofood.fact_` | Regex pattern to whitelist tables to extract | _optional_ |
| `exclude.datasets` | `[]string` | `[dataset_a]` | Dataset IDs to exclude | _optional_ |
| `exclude.tables` | `[]string` | `[dataset_c.table_a]` | Table names in `datasetID.tableID` format to exclude | _optional_ |
| `max_page_size` | `int` | `100` | Page size hint for BigQuery API list calls | _optional_ |
| `dataset_page_size` | `int` | `10` | Page size for listing datasets. Falls back to `max_page_size` | _optional_ |
| `table_page_size` | `int` | `50` | Page size for listing tables. Falls back to `max_page_size` | _optional_ |
| `include_column_profile` | `bool` | `true` | Profile each column (min, max, avg, med, unique, count, top) | _optional_ |
| `max_preview_rows` | `int` | `30` | Number of preview rows to fetch. `0` skips preview, `-1` omits the key entirely. Default `30` | _optional_ |
| `mix_values` | `bool` | `false` | Shuffle column values across preview rows for privacy. Default `false` | _optional_ |
| `build_view_lineage` | `bool` | `true` | Parse view SQL to extract upstream lineage edges. Default `false` | _optional_ |
| `collect_table_usage` | `bool` | `false` | Collect table usage statistics from BigQuery audit logs. Default `false` | _optional_ |
| `usage_period_in_day` | `int` | `7` | Number of days of audit logs to scan. Default `7` | _optional_ |
| `usage_project_ids` | `[]string` | `[my-project]` | GCP project IDs to scan for audit logs. Defaults to `project_id` | _optional_ |
| `concurrency` | `int` | `10` | Number of tables to process concurrently. Default `10` | _optional_ |

### Notes

- Leaving `service_account_json` and `service_account_base64` blank defaults to [Google Application Default Credentials](https://cloud.google.com/docs/authentication/production#automatically). Recommended when Meteor runs inside the same GCP environment.
- The service account needs the `bigquery.privateLogsViewer` role to collect audit logs.

## Entities

### Entity: `table`

**URN format:** `urn:bigquery:{project_id}:table:{project_id}:{dataset_id}.{table_id}`

Produced by `plugins.BigQueryURN(projectID, datasetID, tableID)`.

| Property | Type | Description |
| :------- | :--- | :---------- |
| `entity.description` | `string` | Table description from BigQuery metadata |
| `entity.properties.full_qualified_name` | `string` | Fully qualified table name (`project.dataset.table`) |
| `entity.properties.dataset` | `string` | Dataset ID |
| `entity.properties.project` | `string` | Project ID |
| `entity.properties.type` | `string` | BigQuery table type (`TABLE`, `VIEW`, `MATERIALIZED_VIEW`, etc.) |
| `entity.properties.partition_data` | `object` | Partition configuration (see below) |
| `entity.properties.clustering_fields` | `[]string` | Fields the table is clustered on |
| `entity.properties.sql` | `string` | View SQL query (views and materialized views only) |
| `entity.properties.columns` | `[]object` | Column schema (see below) |
| `entity.properties.profile` | `object` | Table-level usage profile (see below) |
| `entity.properties.create_time` | `string` | Table creation timestamp (RFC 3339) |
| `entity.properties.update_time` | `string` | Last modification timestamp (RFC 3339) |
| `entity.properties.preview_fields` | `[]string` | Column names for preview rows |
| `entity.properties.preview_rows` | `[]array` | Sample data rows |
| `entity.properties.labels` | `map[string]string` | BigQuery table labels |

#### Partition Data (`entity.properties.partition_data`)

| Key | Type | Description |
| :-- | :--- | :---------- |
| `partition_field` | `string` | Partition column. Defaults to `_PARTITIONTIME` for time partitioning with no explicit field |
| `require_partition_filter` | `bool` | Whether queries must filter on the partition column |
| `time_partition.partition_by` | `string` | Time partition granularity: `HOUR`, `DAY`, `MONTH`, `YEAR` |
| `time_partition.partition_expire_seconds` | `float` | Seconds until partition data expires. `0` means no expiry |
| `range_partition.start` | `int` | Range partition start (inclusive) |
| `range_partition.end` | `int` | Range partition end (exclusive) |
| `range_partition.interval` | `int` | Range partition interval width |

#### Column (`entity.properties.columns[]`)

| Key | Type | Description |
| :-- | :--- | :---------- |
| `name` | `string` | Column name |
| `data_type` | `string` | BigQuery data type (`STRING`, `INTEGER`, `RECORD`, etc.) |
| `description` | `string` | Column description |
| `is_nullable` | `bool` | Whether the column is nullable |
| `mode` | `string` | Column mode: `NULLABLE`, `REQUIRED`, or `REPEATED` |
| `policy_tags` | `[]string` | Data Catalog policy tags in `taxonomy:tag:resource` format |
| `columns` | `[]object` | Nested columns (for `RECORD` type) |
| `profile` | `object` | Column profile with `min`, `max`, `avg`, `med`, `unique`, `count`, `top` (when `include_column_profile` is enabled) |

#### Profile (`entity.properties.profile`)

Populated when `collect_table_usage` is enabled.

| Key | Type | Description |
| :-- | :--- | :---------- |
| `total_rows` | `int` | Number of rows in the table |
| `usage_count` | `int` | Number of times the table was queried in the audit log window |
| `common_joins` | `[]object` | Tables commonly joined with this one. Each entry has `urn`, `count`, and `conditions` |
| `filters` | `[]string` | WHERE clause expressions found in queries against this table |

## Edges

| Type | Source | Target | Description |
| :--- | :----- | :----- | :---------- |
| `derived_from` | upstream table URN | this table URN | Upstream dependency parsed from view SQL. Emitted when `build_view_lineage` is enabled and the table is a view or materialized view |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
