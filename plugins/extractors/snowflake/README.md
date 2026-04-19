# Snowflake

Extract table metadata from Snowflake databases.

## Usage

```yaml
source:
  name: snowflake
  config:
    connection_url: user:password@my_organization-my_account/mydb
    exclude:
      databases:
        - database_a
        - database_b
      tables:
        - database_c.table_a
```

## Configuration

| Key                 | Type       | Required | Description                                           |
| :------------------ | :--------- | :------- | :---------------------------------------------------- |
| `connection_url`    | `string`   | Yes      | Snowflake connection URL                               |
| `exclude.databases` | `[]string` | No       | List of databases to exclude from extraction           |
| `exclude.tables`    | `[]string` | No       | List of tables to exclude (format: `database.table`)   |

## Entities

- Entity type: `table`
- Source: `Snowflake`
- URN format: `urn:snowflake:{scope}:table:{database}.{table}`

### Properties

| Property                             | Type     | Description                            |
| :----------------------------------- | :------- | :------------------------------------- |
| `properties.columns`                 | `array`  | List of column metadata                |
| `properties.columns[].name`          | `string` | Column name                            |
| `properties.columns[].data_type`     | `string` | Data type of the column                |
| `properties.columns[].is_nullable`   | `bool`   | Whether the column is nullable         |
| `properties.columns[].description`   | `string` | Column comment (omitted if empty)      |
| `properties.columns[].length`        | `int`    | Character maximum length (omitted if 0)|

## Edges

| Source | Target | Type | Description |
| :----- | :----- | :--- | :---------- |
| `table` | `table` | `references` | Foreign key relationship to the referenced table. |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
