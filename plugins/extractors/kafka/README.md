# kafka

Extract topic metadata from Apache Kafka.

## Usage

```yaml
source:
  name: kafka
  scope: my-kafka-cluster
  config:
    broker: "localhost:9092"
    auth_config:
      tls:
        enabled: true
        insecure_skip_verify: false
        cert_file: "/opt/client.cer.pem"
        key_file: "/opt/client.key.pem"
        ca_file: "/opt/caCertFile.cer.pem"
      sasl:
        enabled: false
        mechanism: "OAUTHBEARER"
```

## Configuration

| Key | Type | Required | Default | Description |
|:----|:-----|:---------|:--------|:------------|
| `broker` | `string` | Yes | | Kafka broker address. |
| `auth_config.tls.enabled` | `bool` | No | `false` | Enable TLS authentication. |
| `auth_config.tls.insecure_skip_verify` | `bool` | No | `false` | Skip server certificate verification. |
| `auth_config.tls.cert_file` | `string` | No | | Path to client certificate file. |
| `auth_config.tls.key_file` | `string` | No | | Path to client key file. |
| `auth_config.tls.ca_file` | `string` | No | | Path to CA certificate file. |
| `auth_config.sasl.enabled` | `bool` | No | `false` | Enable SASL authentication. |
| `auth_config.sasl.mechanism` | `string` | No | | SASL mechanism (e.g. `OAUTHBEARER`). |

## Entities

- **Type:** `topic`
- **URN format:** `urn:kafka:{scope}:topic:{topic_name}`

### Properties

| Property | Type | Description |
|:---------|:-----|:------------|
| `properties.number_of_partitions` | `int64` | Number of partitions for the topic. |

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
