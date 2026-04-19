# Apache Cassandra

Extract table metadata from a Cassandra server.

## Usage

```yaml
source:
  name: cassandra
  config:
    user_id: admin
    password: "1234"
    host: localhost
    port: 9042
    exclude:
      keyspaces: [mykeyspace]
      tables: [mykeyspace_2.tableName_1]
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `user_id` | `string` | Yes | User ID to access the Cassandra server. |
| `password` | `string` | Yes | Password for the Cassandra server. |
| `host` | `string` | Yes | Host address of the Cassandra server. |
| `port` | `int` | Yes | Port number of the Cassandra server. |
| `exclude.keyspaces` | `[]string` | No | List of keyspaces to exclude. System keyspaces are excluded by default. |
| `exclude.tables` | `[]string` | No | List of tables to exclude, in `keyspace.table` format. |

## Entities

- Entity type: `table`
- URN format: `urn:cassandra:{scope}:table:{keyspace}.{table}`

| Property | Type | Description |
| :------- | :--- | :---------- |
| `properties.columns` | `[]object` | List of column objects. |
| `properties.columns[].name` | `string` | Column name. |
| `properties.columns[].data_type` | `string` | Column data type. |

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
