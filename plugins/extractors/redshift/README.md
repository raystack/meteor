# Amazon Redshift

Extract table metadata from Amazon Redshift clusters.

## Usage

```yaml
source:
  name: redshift
  config:
    cluster_id: cluster_test
    db_name: testDB
    db_user: testUser
    aws_region: us-east-1
    exclude:
      databases:
        - secondaryDB
```

## Configuration

| Key          | Type     | Required | Description                                      |
| :----------- | :------- | :------- | :----------------------------------------------- |
| `cluster_id` | `string` | Yes      | Cluster ID to access the Redshift cluster         |
| `db_name`    | `string` | Yes      | Default database name for the Redshift cluster    |
| `db_user`    | `string` | Yes      | Database username for the Redshift cluster         |
| `aws_region` | `string` | Yes      | AWS region of the Redshift cluster                |
| `exclude.databases` | `[]string` | No | List of databases to exclude                    |

Authentication uses the default AWS credential chain (environment variables, shared credentials file, or IAM role).

## Entities

- Entity type: `table`
- Source: `redshift`
- URN format: `urn:redshift:{cluster_id}:table:{cluster_id}.{database}.{table}`

### Properties

| Property                             | Type     | Description                           |
| :----------------------------------- | :------- | :------------------------------------ |
| `properties.columns`                 | `array`  | List of column metadata               |
| `properties.columns[].name`          | `string` | Column name                           |
| `properties.columns[].data_type`     | `string` | Data type of the column               |
| `properties.columns[].is_nullable`   | `bool`   | Whether the column is nullable        |
| `properties.columns[].description`   | `string` | Column label (omitted if empty)       |
| `properties.columns[].length`        | `int`    | Column length (omitted if 0)          |

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
