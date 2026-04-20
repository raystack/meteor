# Apache Kafka

Extract topic and consumer group metadata from Apache Kafka.

## Usage

```yaml
source:
  name: kafka
  scope: my-kafka-cluster
  config:
    broker: "localhost:9092"
    # extract specifies which entity types to extract.
    # Defaults to all: ["topics", "consumer_groups"]
    extract:
      - topics
      - consumer_groups
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
| `extract` | `[]string` | No | `["topics", "consumer_groups"]` | Entity types to extract. |
| `auth_config.tls.enabled` | `bool` | No | `false` | Enable TLS authentication. |
| `auth_config.tls.insecure_skip_verify` | `bool` | No | `false` | Skip server certificate verification. |
| `auth_config.tls.cert_file` | `string` | No | | Path to client certificate file. |
| `auth_config.tls.key_file` | `string` | No | | Path to client key file. |
| `auth_config.tls.ca_file` | `string` | No | | Path to CA certificate file. |
| `auth_config.sasl.enabled` | `bool` | No | `false` | Enable SASL authentication. |
| `auth_config.sasl.mechanism` | `string` | No | | SASL mechanism (e.g. `OAUTHBEARER`). |

## Entities

### Topic

- **Type:** `topic`
- **URN format:** `urn:kafka:{scope}:topic:{topic_name}`

#### Properties

| Property | Type | Description |
|:---------|:-----|:------------|
| `number_of_partitions` | `int64` | Number of partitions for the topic. |
| `replication_factor` | `int64` | Number of replicas per partition. |
| `retention_ms` | `string` | Topic retention period in milliseconds. |
| `cleanup_policy` | `string` | Topic cleanup policy (e.g. `delete`, `compact`). |
| `min_insync_replicas` | `string` | Minimum number of in-sync replicas. |

### Consumer Group

- **Type:** `consumer_group`
- **URN format:** `urn:kafka:{scope}:consumer_group:{group_id}`

#### Properties

| Property | Type | Description |
|:---------|:-----|:------------|
| `state` | `string` | Consumer group state (e.g. `Stable`, `Empty`). |
| `protocol` | `string` | Partition assignment protocol. |
| `protocol_type` | `string` | Protocol type (e.g. `consumer`). |
| `num_members` | `int64` | Number of members in the group. |
| `members` | `[]object` | List of group members with `member_id`, `client_id`, and `host`. |

## Edges

| Source | Target | Type | Description |
|:-------|:-------|:-----|:------------|
| `consumer_group` | `topic` | `consumed_by` | Consumer group consumes from a topic. |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.mdx#adding-a-new-extractor) for information on contributing to this module.
