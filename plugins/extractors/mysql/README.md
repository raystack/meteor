# mysql

## Usage

```yaml
source:
  name: mysql
  config:
    connection_url: admin:pass123@tcp(localhost:3306)/
    instance_url: my-mysql
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `connection_url` | `string` | `admin:pass123@tcp(localhost:3306)/` | URL to access the mysql server | *required* |
| `identifier` | `string` | `my-mysql` | Instance alias, the value will be used as part of the urn component | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `mysql::my-mysql/my_database/my_table` |
| `resource.name` | `my_table` |
| `resource.service` | `mysql` |
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
