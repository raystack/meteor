# Apache Kafka

## Usage

```yaml
sinks:
  - name: kafka
    config:
      brokers: "localhost:9092"
      topic: metadata-topic
      key_path: ".resource.urn"
```

## Config

| Key | Value | Example | Description | |
| :-- | :---- | :------ | :---------- | :- |
| `brokers` | `string` | `localhost:9092` | Comma-separated list of Kafka broker addresses | *required* |
| `topic` | `string` | `metadata-topic` | Kafka topic to publish messages to | *required* |
| `key_path` | `string` | `.resource.urn` | JSON path to extract the Kafka message key from the asset. Only supports root level keys. | *optional* |

## Behavior

- Each asset is serialized as a JSON message and published to the configured Kafka topic.
- If `key_path` is set, the value at that path in the asset is used as the Kafka message key. This is useful for partitioning messages by asset URN or type.
- If `key_path` is not set, messages are published without a key and will be distributed across partitions by the Kafka producer.

## Examples

### Basic metadata streaming

```yaml
name: bigquery-to-kafka
version: v1beta1
source:
  name: bigquery
  config:
    project_id: my-project
sinks:
  - name: kafka
    config:
      brokers: "broker1:9092,broker2:9092"
      topic: metadata-stream
      key_path: ".resource.urn"
```

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-sink) for information on contributing to this module.
