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

## Big Query Table

`bigquerytable`

### Configs
| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `project_id` | `string` | `my-project` | BigQuery Project ID | *required* |
| `service_account_json` | `string` | `{"private_key": .., "private_id": ...}` | Service Account in JSON string | *optional* |

#### *Notes*
Leaving `service_account_json` blank will default to [Google's default authentication](https://cloud.google.com/docs/authentication/production#automatically). It is recommended if Meteor instance runs inside the same Google Cloud environment as the BigQuery project.

## MongoDB

`mongodb`

### Configs
| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `user_id` | `string` | `user` | User ID to access the mongo server| *required* |
| `password` | `string` | `abcd` | Password for the Mongo Server | *required* |
| `host` | `string` | `localhost:27017` | The Host at which server is running | *required* |

## MySQL

`mysql`

### Configs
| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `user_id` | `string` | `user` | User ID to access the mysql server| *required* |
| `password` | `string` | `abcd` | Password for the mysql Server | *required* |
| `host` | `string` | `localhost:27017` | The Host at which server is running | *required* |

## Postgres-sql

`postgres`

### Configs
| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `user_id` | `string` | `user` | User ID to access the postgres server| *required* |
| `password` | `string` | `abcd` | Password for the postgres Server | *required* |
| `host` | `string` | `localhost:27017` | The Host at which server is running | *required* |
| `database_name` | `string` | `postgres` | The Database owned by user mentioned in Config, root user can skip | *optional* |