# snowflake

## Usage

```yaml
source:
  type: snowflake
  config:
    connection_url: user:password@my_organization-my_account/mydb
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `connection_url` | `string` | `user:password@org22-acc123/mydb` | URL to access the snowflake server | *required* |

## Outputs

| Field              | Sample Value           |
|:-------------------|:-----------------------|
| `resource.urn`     | `my_database.my_table` |
| `resource.name`    | `my_table`             |
| `resource.service` | `snowflake`            |
| `description`      | `table description`    |
| `schema`           | [][Column](#column)    |

### Column

| Field         | Sample Value         |
|:--------------|:---------------------|
| `name`        | `total_price`        |
| `description` | `item's total price` |
| `data_type`   | `decimal`            |
| `is_nullable` | `true`               |
| `length`      | `11`                 |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
