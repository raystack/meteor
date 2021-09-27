# clickhouse

## Usage

```yaml
source:
  type: clickhouse
  config:
    host: localhost:9000
    user_id: admin
    password: 1234
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `host` | `string` | `localhost:9000` | The Host at which server is running | *required* |
| `user_id` | `string` | `admin` | User ID to access the clickhouse server| *required* |
| `password` | `string` | `1234` | Password for the clickhouse Server | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `my_database.my_table` |
| `resource.name` | `my_table` |
| `resource.service` | `clickhouse` |
| `description` | `table description` |
| `profile.total_rows` | `2100` |
| `schema` | [][Column](#column) |

### Column

| Field | Sample Value |
| :---- | :---- |
| `name` | `total_price` |
| `description` | `item's total price` |
| `data_type` | `String` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
