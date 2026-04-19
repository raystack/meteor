# ClickHouse

Extract table metadata from a ClickHouse server.

## Usage

```yaml
source:
  name: clickhouse
  config:
    connection_url: tcp://localhost:9000?username=admin&password=pass123&debug=true
    exclude:
      databases: [database_a, database_b]
      tables: [database_c.table_a]
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `connection_url` | `string` | Yes | ClickHouse connection URL. |
| `exclude.databases` | `[]string` | No | List of databases to exclude. |
| `exclude.tables` | `[]string` | No | List of tables to exclude, in `database.table` format. |

## Entities

- Entity type: `table`
- URN format: `urn:clickhouse:{scope}:table:{database}.{table}`

| Property | Type | Description |
| :------- | :--- | :---------- |
| `properties.columns` | `[]object` | List of column objects. |
| `properties.columns[].name` | `string` | Column name. |
| `properties.columns[].data_type` | `string` | Column data type. |
| `properties.columns[].description` | `string` | Column description (if available). |

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
