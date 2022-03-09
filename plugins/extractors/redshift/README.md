# redshift

## Usage

```yaml
source:
  name: redshift
  config:
    cluster_id: cluster_test
    db_name: testDB
    db_user: testUser
    aws_region: us-east-1
    exclude: secondaryDB
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `cluster_id` | `string` | `cluster_test` | cluster ID to access the redshift cluster | *required* |
| `db_name` | `string` | `testDB` | Default database name to access the redshift cluster | *required* |
| `db_user` | `string` | `testUser` | Database username to access the redshift cluster | *required* |
| `aws_region` | `string` | `us-east-1` | Aws region to access the redshift cluster | *required* |
| `exclude` | `string` | `secondaryDB` | This is a comma separated db list | *optional* |

## Outputs

| Field                | Sample Value                               |
|:---------------------|:-------------------------------------------|
| `resource.urn`       | `redshift::us-east-1/my_database/my_table` |
| `resource.name`      | `my_table`                                 |
| `resource.service`   | `redshift`                                 |
| `schema`             | [][Column](#column)                        |

### Column

| Field         | Sample Value         |
|:--------------|:---------------------|
| `name`        | `total_price`        |
| `description` | `item's total price` |
| `data_type`   | `decimal`            |
| `is_nullable` | `true`               |
| `length`      | `1243`               |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
