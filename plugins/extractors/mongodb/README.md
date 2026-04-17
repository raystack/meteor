# mongodb

Extract collection metadata from a MongoDB server.

## Usage

```yaml
source:
  name: mongodb
  config:
    connection_url: mongodb://admin:pass123@localhost:27017
    exclude:
      databases:
        - database_a
        - database_b
      collections:
        - database_c.collection_a
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `connection_url` | `string` | Yes | MongoDB connection URI. |
| `exclude.databases` | `[]string` | No | List of database names to exclude. |
| `exclude.collections` | `[]string` | No | List of collections to exclude, in `database.collection` format. Default system collections are always excluded. |

## Entities

- Entity type: `table`
- URN format: `urn:mongodb:{scope}:collection:{database}.{collection}`

| Property | Type | Description |
| :------- | :--- | :---------- |
| `properties.profile.total_rows` | `int` | Estimated document count (omitted if zero). |

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
