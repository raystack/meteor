# kafka

## Usage

```yaml
source:
  name: kafka
  config:
    broker: "localhost:9092"
    label: "my-kafka-cluster"
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `broker` | `string` | `localhost:9092` | Kafka broker's host | *required* |
| `label` | `string` | `samplePrefix` | Label will be used as a part in Urn components | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `my-topic` |
| `resource.name` | `my-topic` |
| `resource.service` | `kafka` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
