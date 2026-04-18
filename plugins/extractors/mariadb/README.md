# MariaDB

Extract table metadata from a MariaDB server.

## Usage

```yaml
source:
  name: mariadb
  config:
    connection_url: admin:pass123@tcp(localhost:3306)/
    exclude:
      databases:
        - database_a
        - database_b
      tables:
        - database_c.table_a
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `connection_url` | `string` | Yes | MariaDB connection URL. |
| `exclude.databases` | `[]string` | No | List of databases to exclude. |
| `exclude.tables` | `[]string` | No | List of tables to exclude (`database.table` format). |

## Entities

- **Type:** `table`
- **URN format:** `urn:mariadb:{scope}:table:{database}.{table}`

### Properties

| Property | Type | Description |
| :------- | :--- | :---------- |
| `properties.columns` | `[]object` | List of column metadata objects. |

#### Column object

| Field | Type | Description |
| :---- | :--- | :---------- |
| `name` | `string` | Column name. |
| `description` | `string` | Column comment (omitted when empty). |
| `data_type` | `string` | Data type of the column. |
| `is_nullable` | `bool` | Whether the column is nullable. |
| `length` | `int` | Maximum character length (omitted when 0). |

## Edges

| Source | Target | Type | Description |
| :----- | :----- | :--- | :---------- |
| `table` | `table` | `references` | Foreign key relationship to the referenced table. |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
