# presto

## Usage

```yaml
source:
  name: presto
  config:
    connection_url: http://user:pass@localhost:8080?catalog=default&schema=test
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `connection_url` | `string` | `http://user:pass@localhost:8080?catalog=default&schema=test` | URL to access the presto server | *required* |

## Outputs

| Field                | Sample Value           |
|:---------------------|:-----------------------|
| `resource.urn`       | `my_database.my_table` |
| `resource.name`      | `my_table`             |
| `resource.service`   | `presto`               |
| `description`        | `table description`    |
| `profile.total_rows` | `1100`                 |
| `schema`             | [][Column](#column)    |

### Column

| Field         | Sample Value         |
|:--------------|:---------------------|
| `name`        | `total_price`        |
| `description` | `item's total price` |
| `data_type`   | `decimal`            |
| `is_nullable` | `true`               |
| `length`      | `11`                 |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
