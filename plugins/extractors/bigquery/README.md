# bigquery

## Usage
```yaml
source:
  type: bigquery
  config:
    project_id: google-project-id
    table_pattern: gofood.fact_
    profile_column: true
    credentials_json:
      {
        "type": "service_account",
        "private_key_id": "xxxxxxx",
        "private_key": "xxxxxxx",
        "client_email": "xxxxxxx",
        "client_id": "xxxxxxx",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "xxxxxxx",
        "client_x509_cert_url": "xxxxxxx"
      }
```
## Inputs
| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `project_id` | `string` | `my-project` | BigQuery Project ID | *required* |
| `credentials_json` | `string` | `{"private_key": .., "private_id": ...}` | Service Account in JSON string | *optional* |
| `table_pattern` | `string` | `gofood.fact_` | Regex pattern to filter which bigquery table to scan (whitelist) | *optional* |
| `profile_column` | `bool` | `true` | true if you want to profile the column value such min, max, med, avg, top, and freq | *optional* |

### *Notes*
Leaving `credentials_json` blank will default to [Google's default authentication](https://cloud.google.com/docs/authentication/production#automatically). It is recommended if Meteor instance runs inside the same Google Cloud environment as the BigQuery project.

## Outputs
| Field | Sample Value |
| :---- | :---- |
| `urn` | `project_id.dataset_name.table_name` |
| `name` | `table_name` |
| `source` | `bigquery` |
| `description` | `table description` |
| `profile.total_rows` | `2100` |
| `schema` | [][Column](#column) |

### Column
| Field | Sample Value |
| :---- | :---- |
| `name` | `total_price` |
| `description` | `item's total price` |
| `data_type` | `decimal` |
| `is_nullable` | `true` |
| `length` | `12,2` |
| `profile` | `{"min":...,"max": ...,"unique": ...}` |

## Contributing
Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
