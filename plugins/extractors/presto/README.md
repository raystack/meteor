# PrestoDB

Extract table metadata from a Presto server.

## Usage

```yaml
source:
  name: presto
  config:
    connection_url: http://user:pass@localhost:8080
    exclude:
      catalogs:
        - memory
        - system
        - tpcds
        - tpch
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `connection_url` | `string` | Yes | HTTP URL to access the Presto server. |
| `exclude.catalogs` | `[]string` | No | List of catalog names to exclude. |

## Entities

- Entity type: `table`
- URN format: `urn:presto:{scope}:table:{catalog}.{schema}.{table}`

| Property | Type | Description |
| :------- | :--- | :---------- |
| `properties.columns` | `[]object` | List of column objects. |
| `properties.columns[].name` | `string` | Column name. |
| `properties.columns[].data_type` | `string` | Column data type. |
| `properties.columns[].is_nullable` | `bool` | Whether the column is nullable. |
| `properties.columns[].description` | `string` | Column comment (if available). |

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
