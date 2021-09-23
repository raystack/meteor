# mariadb

## Usage

```yaml
source:
  type: mariadb
  config:
    user_id: admin
    password: 1234
    host: localhost:3306
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `user_id` | `string` | `admin` | User ID to access the mariadb server| *required* |
| `password` | `string` | `1234` | Password for the mariadb Server | *required* |
| `host` | `string` | `localhost:3306` | The Host at which server is running | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `my_database.my_table` |
| `resource.name` | `my_table` |
| `resource.service` | `mariadb` |
| `description` | `table description` |
| `profile.total_rows` | `1100` |
| `schema` | [][Column](#column) |

### Column

| Field | Sample Value |
| :---- | :---- |
| `name` | `total_price` |
| `description` | `item's total price` |
| `data_type` | `decimal` |
| `is_nullable` | `true` |
| `length` | `11` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
