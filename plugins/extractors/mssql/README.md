# Microsoft SQL Server

Extract table metadata from Microsoft SQL Server databases.

## Usage

```yaml
source:
  name: mssql
  config:
    connection_url: sqlserver://admin:pass123@localhost:1433/
    exclude:
      databases:
        - database_a
        - database_b
      tables:
        - database_c.table_a
```

## Configuration

| Key                 | Type       | Required | Description                                          |
| :------------------ | :--------- | :------- | :--------------------------------------------------- |
| `connection_url`    | `string`   | Yes      | URL to access the MSSQL server                       |
| `exclude.databases` | `[]string` | No       | List of databases to exclude from extraction          |
| `exclude.tables`    | `[]string` | No       | List of tables to exclude (format: `database.table`)  |

System databases `master`, `msdb`, `model`, and `tempdb` are excluded by default.

## Entities

- Entity type: `table`
- Source: `mssql`
- URN format: `urn:mssql:{scope}:table:{database}.{table}`

### Properties

| Property                        | Type     | Description              |
| :------------------------------ | :------- | :----------------------- |
| `properties.columns`            | `array`  | List of column metadata  |
| `properties.columns[].name`     | `string` | Column name              |
| `properties.columns[].data_type`| `string` | Data type of the column  |
| `properties.columns[].is_nullable` | `bool` | Whether the column is nullable |
| `properties.columns[].length`   | `int`    | Character maximum length (omitted if 0) |

## Edges

| Source | Target | Type | Description |
| :----- | :----- | :--- | :---------- |
| `table` | `table` | `references` | Foreign key relationship to the referenced table. |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.mdx#adding-a-new-extractor) for information on contributing to this module.
