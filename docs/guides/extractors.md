# Extractors

## Kafka

`kafka`

### Configs
| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `broker` | `string` | `localhost:9092` | Broker Host | *required* |

## Big Query Dataset

`bigquerydataset`

### Configs
| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `project_id` | `string` | `my-project` | BigQuery Project ID | *required* |
| `service_account_json` | `string` | `{"private_key": .., "private_id": ...}` | Service Account in JSON string | *optional* |

#### *Notes*
Leaving `service_account_json` blank will default to [Google's default authentication](https://cloud.google.com/docs/authentication/production#automatically). It is recommended if Meteor instance runs inside the same Google Cloud environment as the BigQuery project.
