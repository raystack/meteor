# oracle

Extract table metadata from Oracle databases.

## Usage

```yaml
source:
  name: oracle
  config:
    connection_url: oracle://admin:1234@localhost:1521/xe
```

## Configuration

| Key              | Type     | Required | Description                              |
| :--------------- | :------- | :------- | :--------------------------------------- |
| `connection_url` | `string` | Yes      | Connection string to access Oracle Database |

## Entities

- Entity type: `table`
- Source: `Oracle`
- URN format: `urn:oracle:{scope}:table:{database}.{table}`

### Properties

| Property                             | Type     | Description                          |
| :----------------------------------- | :------- | :----------------------------------- |
| `properties.columns`                 | `array`  | List of column metadata              |
| `properties.columns[].name`          | `string` | Column name                          |
| `properties.columns[].data_type`     | `string` | Data type of the column              |
| `properties.columns[].is_nullable`   | `bool`   | Whether the column is nullable       |
| `properties.columns[].description`   | `string` | Column comment (omitted if empty)    |
| `properties.columns[].length`        | `int`    | Data length (omitted if 0)           |
| `properties.profile.total_rows`      | `int`    | Total number of rows in the table (omitted if 0) |

## Edges

| Source | Target | Type | Description |
| :----- | :----- | :--- | :---------- |
| `table` | `table` | `references` | Foreign key relationship to the referenced table. |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
