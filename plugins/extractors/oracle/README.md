# Oracle

## Usage

```yaml
source:
  type: oracle
  config:
    connection_url: oracle://admin:1234@localhost:1521/xe
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `connection_url` | `string` | `oracle://admin:1234@localhost:1521/xe` | Connection String to access Oracle Database | *required* |

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
