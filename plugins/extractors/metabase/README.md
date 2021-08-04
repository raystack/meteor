# mysql

## Usage

```yaml
source:
  type: metabase
  config:
    host: http://localhost:3000
    user_id: meteor_tester
    password: meteor_pass_1234
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `host` | `string` | `http://localhost:3000` | The url at which metabase is running | *required* |
| `user_id` | `string` | `meteor_tester` | User ID to access the metabase| *required* |
| `password` | `string` | `meteor_pass_1234` | Password for the metabase | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `urn` | `my_database.my_table` |
| `name` | `my_table` |
| `source` | `mysql` |
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
