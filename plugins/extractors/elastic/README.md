# Elasticsearch

Extract index metadata from an Elasticsearch cluster.

## Usage

```yaml
source:
  name: elastic
  config:
    host: http://localhost:9200
    user: elastic
    password: changeme
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `host` | `string` | Yes | Host address of the Elasticsearch server (include scheme). |
| `user` | `string` | No | Username for authentication. |
| `password` | `string` | No | Password for authentication. |

## Entities

- Entity type: `table`
- URN format: `urn:elasticsearch:{scope}:index:{index_name}`

| Property | Type | Description |
| :------- | :--- | :---------- |
| `properties.columns` | `[]object` | List of column objects from the index mapping. |
| `properties.columns[].name` | `string` | Field name. |
| `properties.columns[].data_type` | `string` | Elasticsearch field type (e.g. `text`, `keyword`). |
| `properties.profile.total_rows` | `int` | Number of documents in the index (omitted if zero). |
| `properties.number_of_shards` | `string` | Number of primary shards for the index. |
| `properties.number_of_replicas` | `string` | Number of replica shards for the index. |

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
