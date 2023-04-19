# mssql

## Usage

```yaml
source:
  name: mssql
  config:
    connection_url: sqlserver://admin:pass123@localhost:3306/
    exclude:
      databases:
        - database_a
        - database_b
      tables:
        - database_c.table_a
```

## Inputs

| Key                 | Value      | Example                                        | Description                      |            |
| :------------------ | :--------- | :--------------------------------------------- | :------------------------------- | :--------- |
| `connection_url`    | `string`   | `sqlserver://admin:pass123@localhost:3306/`    | URL to access the mssql server   | _required_ |
| `exclude.databases` | `[]string` | `[`database_a`, `database_b`]`                 | List of databases to be excluded | _optional_ |
| `exclude.tables`    | `[]string` | `[`database_c.table_a`, `database_c.table_b`]` | List of tables to be excluded    | _optional_ |

## Outputs

| Field                | Sample Value                           |
| :------------------- | :------------------------------------- |
| `resource.urn`       | `mssql::my-mssql/my_database/my_table` |
| `resource.name`      | `my_table`                             |
| `resource.service`   | `mssql`                                |
| `description`        | `table description`                    |
| `profile.total_rows` | `2100`                                 |
| `schema`             | [][column](#column)                    |

### Column

| Field         | Sample Value         |
| :------------ | :------------------- |
| `name`        | `total_price`        |
| `description` | `item's total price` |
| `data_type`   | `decimal`            |
| `is_nullable` | `true`               |
| `length`      | `12,2`               |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
