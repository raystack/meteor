# postgres

## Usage

```yaml
source:
  type: postgres
  config:
    connection_url: postgres://admin:pass123@localhost:3306/testDB?sslmode=disable
    database_name: testDB
    exclude: postgres
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `connection_url` | `string` | `postgres://admin:pass123@localhost:3306/testDB?sslmode=disable` | URL to access the postgres server | *required* |
| `exclude` | `string` | `postgres` | The Database want to be ignored | *optional* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `postgres::localhost:3306/my_database/my_table` |
| `resource.name` | `my_table` |
| `resource.service` | `postgres` |
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

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
