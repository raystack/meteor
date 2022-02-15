# redshift

## Usage

```yaml
source:
  name: redshift
  config:
    connection_url: 
    exclude: 
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `connection_url` | `string` | `` | URL to access the redshift server | *required* |
| `exclude` | `string` | `` | This is a comma separated db list | *optional* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `redshift::localhost:3306/my_database/my_table` |
| `resource.name` | `my_table` |
| `resource.service` | `redshift` |
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
