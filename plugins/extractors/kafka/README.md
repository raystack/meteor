# kafka

## Usage

```yaml
source:
  name: kafka
  config:
    broker: "localhost:9092"
    auth_config:
      tls:
        enabled: true
        insecure_skip_verify: false
        cert_file: "/opt/client.cer.pem"
        key_file: "/opt/client.key.pem"
        ca_file: "/opt/caCertFile.cer.pem"
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `broker` | `string` | `localhost:9092` | Kafka broker's host | *required* |
| `auth_config.tls.enabled` | `boolean` | `false` | config to enable tls auth | *optional* |
| `auth_config.tls.insecure_skip_verify` | `boolean` | `false` | InsecureSkipVerify controls whether a client verifies the server's certificate chain and host name | *optional* |
| `auth_config.tls.cert_file` | `string` | `/opt/client.cer.pem` | certificate file for client authentication | *optional* |
| `auth_config.tls.key_file` | `string` | `/opt/client.key.pem` | key file for client authentication | *optional* |
| `auth_config.tls.ca_file` | `string` | `/opt/caCertFile.cer.pem` | certificate authority file for TLS client authentication | *optional* |


## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `my-topic` |
| `resource.name` | `my-topic` |
| `resource.service` | `kafka` |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
