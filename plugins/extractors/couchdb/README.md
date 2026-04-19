# Apache CouchDB

Extract document metadata from a CouchDB server.

## Usage

```yaml
source:
  name: couchdb
  config:
    connection_url: http://admin:pass123@localhost:5984/
    exclude:
      databases:
        - database_a
        - database_b
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `connection_url` | `string` | Yes | URL to access the CouchDB server. |
| `exclude.databases` | `[]string` | No | List of database names to exclude. Internal databases (`_global_changes`, `_replicator`, `_users`) are excluded by default. |

## Entities

- Entity type: `table`
- URN format: `urn:couchdb:{scope}:table:{database}.{docID}`

| Property | Type | Description |
| :------- | :--- | :---------- |
| `properties.columns` | `[]object` | List of column objects derived from document fields. |
| `properties.columns[].name` | `string` | Field name. |
| `properties.columns[].data_type` | `string` | Inferred Go type of the field value (e.g. `float64`, `string`). |
| `properties.columns[].description` | `string` | Document revision string (if available). |
| `properties.columns[].length` | `int` | Document size in bytes (if non-zero). |

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
