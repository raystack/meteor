# mysql

## Usage

```yaml
source:
  name: mysql
  config:
    connection_url: admin:pass123@tcp(localhost:3306)/
    exclude:
      databases:
        - database_a
        - database_b
      tables:
        - database_c.table_a
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `connection_url` | `string` | `admin:pass123@tcp(localhost:3306)/` | URL to access the mysql server | *required* |
| `exclude.databases` | `[]string` | `[`database_a`, `database_b`]` | List of databases to be excluded | *optional* |
| `exclude.tables` | `[]string` | `[`database_c.table_a`, `database_c.table_b`]` | List of tables to be excluded | *optional* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `mysql::my-mysql/my_database/my_table` |
| `resource.name` | `my_table` |
| `resource.service` | `mysql` |
| `description` | `table description` |
| `profile.total_rows` | `2100` |
| `schema` | [][Column](#column) |

### Column

| Field | Sample Value |
| :---- | :---- |
| `name` | `total_price` |
| `description` | `item's total price` |
| `data_type` | `decimal` |
| `is_nullable` | `true` |
| `length` | `12,2` |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
