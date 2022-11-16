# presto

## Usage

```yaml
source:
  name: presto
  config:
    connection_url: http://user:pass@localhost:8080
    exclude_catalog: memory,system
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `connection_url` | `string` | `http://user:pass@localhost:8080` | URL to access the presto server | *required* |
| `exclude_catalog` | `string` | `memory,system` | This is a comma separated catalog list to exclude from querying data | *optional* |

## Outputs

| Field                | Sample Value                      |
|:---------------------|:----------------------------------|
| `resource.urn`       | `my_catalog.my_database.my_table` |
| `resource.name`      | `my_table`                        |
| `resource.service`   | `presto`                          |
| `schema`             | [][Column](#column)               |

### Column

| Field         | Sample Value         |
|:--------------|:---------------------|
| `name`        | `total_price`        |
| `data_type`   | `decimal`            |
| `is_nullable` | `true`               |
| `description` | `item's total price` |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
