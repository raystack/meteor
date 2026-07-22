# Kafka

Publish metadata records as JSON messages to an Apache Kafka topic.

## Usage

```yaml
sinks:
  - name: kafka
    config:
      brokers: "localhost:9092"
      topic: metadata-topic
      key_path: ".Urn"
```

## Configuration

| Key | Type | Example | Description | |
| :-- | :--- | :------ | :---------- | :- |
| `brokers` | `string` | `localhost:9092` | Comma-separated list of Kafka broker addresses | *required* |
| `topic` | `string` | `metadata-topic` | Kafka topic to publish messages to | *required* |
| `key_path` | `string` | `.Urn` | Path to the Entity field used as the Kafka message key. Only top-level fields are supported (e.g. `.Urn`, `.Type`, `.Name`). | *optional* |

## Behavior

The message value is the whole Record serialized as JSON (`{"entity": {...}, "edges": [...]}`, snake_case keys, `edges` omitted when empty) and published to the configured Kafka topic.

If `key_path` is set, the value of that Entity field is protobuf-encoded and used as the Kafka message key, which controls partition assignment. If omitted, messages are published without a key and distributed across partitions by the producer.

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.mdx#adding-a-new-sink) for information on contributing to this module.
