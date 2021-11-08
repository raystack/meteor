# optimus

## Usage

```yaml
source:
  type: optimus
  config:
    host: https://optimus-host.com
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `host` | `string` | `https://optimus-host.com` | Optimus' host | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `optimus::https://optimus-host.com/project.namespace.job` |
| `resource.name` | `job-name` |
| `resource.service` | `optimus` |
| `resource.description` | `Sample job description` |
| `ownership.owners[0].urn` | `john_doe@example.com` |
| `ownership.owners[0].name` | `john_doe@example.com` |
| `lineage.upstreams[].urn` | `bigquery::project/dataset/table` |
| `lineage.upstreams[].type` | `table` |
| `lineage.upstreams[].service` | `bigquery` |
| `lineage.downstreams[0].urn` | `bigquery::project/dataset/table` |
| `lineage.downstreams[0].type` | `table` |
| `lineage.downstreams[0].service` | `bigquery` |
| `properties.attributes` | `{}` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
