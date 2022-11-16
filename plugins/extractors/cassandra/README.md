# cassandra

## Usage

```yaml
source:
  name: cassandra
  config:
    user_id: admin
    password: 1234
    host: localhost
    port: 9042
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `user_id` | `string` | `admin` | User ID to access the cassandra server| *required* |
| `password` | `string` | `1234` | Password for the cassandra Server | *required* |
| `host` | `string` | `127.0.0.1` | The Host address at which server is running | *required* |
| `port` | `int` | `9042` | The Port number at which server is running | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `my_keyspace.my_table` |
| `resource.name` | `my_table` |
| `resource.service` | `cassandra` |
| `description` | `table description` |
| `profile.total_rows` | `2100` |
| `schema` | [][Column](#column) |

### Column

| Field | Sample Value |
| :---- | :---- |
| `name` | `total_price` |
| `type` | `text` |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
