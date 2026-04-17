# Postgres

Extract table metadata from a PostgreSQL server.

## Usage

```yaml
source:
  name: postgres
  config:
    connection_url: postgres://admin:pass123@localhost:5432/postgres?sslmode=disable
    exclude: testDB,secondaryDB
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `connection_url` | `string` | Yes | PostgreSQL connection URL. |
| `exclude` | `string` | No | Comma-separated list of databases to exclude. |

## Entities

- **Type:** `table`
- **URN format:** `urn:postgres:{scope}:table:{database}.{table}`

### Properties

| Property | Type | Description |
| :------- | :--- | :---------- |
| `properties.columns` | `[]object` | List of column metadata objects. |
| `properties.grants` | `[]object` | List of user privilege grants (when available). |

#### Column object

| Field | Type | Description |
| :---- | :--- | :---------- |
| `name` | `string` | Column name. |
| `data_type` | `string` | Data type of the column. |
| `is_nullable` | `bool` | Whether the column is nullable. |
| `length` | `int` | Maximum character length (omitted when 0). |

#### Grant object

| Field | Type | Description |
| :---- | :--- | :---------- |
| `user` | `string` | Grantee name. |
| `privilege_types` | `[]string` | List of granted privileges (e.g. `SELECT`, `INSERT`). |

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
