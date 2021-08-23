# elastic search

## Usage

```yaml
source:
  type: elasticsearch
  config:
    base_url: elastic_server
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `url` | `string` | `http://localhost:9200` | URL of the Elastic server | *required* |
| `user` | `string` | `admin` | User ID to access the server| *optional* |
| `password` | `string` | `1234` | Password for the Server | *optional* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `urn` | `elasticsearch.index1` |
| `name` | `index1` |
| `profile.total_rows` | `1` |
| `schema` | [][Column](#column) |

### Column

| Field | Sample Value |
| :---- | :---- |
| `name` | `SomeStr` |
| `data_type` | `text` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
