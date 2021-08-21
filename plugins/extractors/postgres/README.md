# postgres

## Usage

```yaml
source:
  type: postgres
  config:
    host: localhost:5432
    user_id: admin
    password: 1234
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `host` | `string` | `localhost:5432` | The Host at which server is running | *required* |
| `user_id` | `string` | `admin` | User ID to access the postgres server| *required* |
| `password` | `string` | `1234` | Password for the postgres Server | *required* |
| `database_name` | `string` | `postgres` | The Database owned by user mentioned in Config, root user can skip | *optional* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `urn` | `my_database.my_table` |
| `name` | `my_table` |
| `source` | `postgres` |
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
