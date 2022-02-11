# mongodb

## Usage

```yaml
source:
  name: mongodb
  config:
    connection_url: mongodb://admin:pass123@localhost:3306
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `connection_url` | `string` | `mongodb://admin:pass123@localhost:3306` | URL to access the mongodb server | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `my_database.my_collection` |
| `resource.name` | `my_collection` |
| `resource.service` | `mongodb` |
| `description` | `table description` |
| `profile.total_rows` | `2100` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
