# google cloud storage

## Usage

```yaml
source:
  name: googlecloudstorage
  config:
    project_id: google-project-id
    extract_blob: true
    # Only one of service_account_base64 / service_account_json is needed. 
    # If both are present, service_account_base64 takes precedence
    service_account_base64: _________BASE64_ENCODED_SERVICE_ACCOUNT_________________
    service_account_json:
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

| Key                      | Value     | Example                                  | Description                                                                                  |            |
|:-------------------------|:----------|:-----------------------------------------|:---------------------------------------------------------------------------------------------|:-----------|
| `project_id`             | `string`  | `my-project`                             | BigQuery Project ID                                                                          | *required* |
| `extract_blob`           | `boolean` | `true`                                   | Extract blob metadata inside a bucket                                                        | *optional* |
| `service_account_base64` | `string`  | `____BASE64_ENCODED_SERVICE_ACCOUNT____` | Service Account in base64 encoded string. Takes precedence over `service_account_json` value | *optional* |
| `service_account_json`   | `string`  | `{"private_key": .., "private_id": ...}` | Service Account in JSON string                                                               | *optional* |

### *Notes*

Leaving `service_account_base64` and `service_account_json` blank will default
to [Google's default authentication](https://cloud.google.com/docs/authentication/production#automatically).
It is recommended if Meteor instance runs inside the same Google Cloud
environment as the Google Cloud Storage project.

## Outputs

| Field                           | Sample Value             |
|:--------------------------------|:-------------------------|
| `resource.urn`                  | `project_id/bucket_name` |
| `resource.name`                 | `bucket_name`            |
| `resource.service`              | `googlecloudstorage`     |
| `location`                      | `ASIA`                   |
| `storage_type`                  | `STANDARD`               |
| `labels`                        | []{`key`:`value`}        |
| `timestamps.created_at.seconds` | `1551082913`             |
| `timestamps.created_at.nanos`   | `1551082913`             |

### Blob

| Field                           | Sample Value                                                |
|:--------------------------------|:------------------------------------------------------------|
| `urn`                           | `project_id/bucket_name/blob_path`                          |
| `name`                          | `blob_path`                                                 |
| `size`                          | `311`                                                       |
| `deleted_at.seconds`            | `1551082913`                                                |
| `expired_at.seconds`            | `1551082913`                                                |
| `labels`                        | []{`key`:`value`}                                           |
| `ownership.owners`              | []{`name`:`serviceaccountname@project.gserviceaccount.com`} |
| `timestamps.created_at.seconds` | `1551082913`                                                |
| `timestamps.created_at.nanos`   | `1551082913`                                                |
| `timestamps.updated_at.seconds` | `1551082913`                                                |
| `timestamps.updated_at.nanos`   | `1551082913`                                                |

## Contributing

Refer to
the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor)
for information on contributing to this module.
