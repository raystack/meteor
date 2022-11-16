# clickhouse

## Usage

```yaml
source:
  name: clickhouse
  config:
    connection_url: tcp://localhost:3306?username=admin&password=pass123&debug=true
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `connection_url` | `string` | `tcp://localhost:3306?username=admin&password=pass123&debug=true` | URL to access the clickhouse server | *required* |

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

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
