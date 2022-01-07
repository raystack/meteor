# Oracle

## Usage

```yaml
source:
  type: oracle
  config:
    host: localhost:1521
    user_id: admin
    password: 1234
    database: xe
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `host` | `string` | `localhost:1521` | The Host at which server is running | *required* |
| `user_id` | `string` | `admin` | User ID to access the Oracle server| *required* |
| `password` | `string` | `1234` | Password for the Oracle Server | *required* |
| `database` | `string` | `xe` | The Database owned by user mentioned in Config | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `my_database.my_table` |
| `resource.name` | `my_table` |
| `resource.service` | `Oracle` |
| `profile.total_rows` | `2100` |
| `schema` | [][Column](#column) |

### Column

| Field | Sample Value |
| :---- | :---- |
| `name` | `NAME` |
| `data_type` | `VARCHAR2` |
| `is_nullable` | `true` |
| `length` | `255` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
