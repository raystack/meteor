# gcs

Extract bucket and blob metadata from Google Cloud Storage.

## Usage

```yaml
source:
  name: gcs
  config:
    project_id: google-project-id
    extract_blob: true
    # Only one of service_account_base64 / service_account_json is needed.
    # If both are present, service_account_base64 takes precedence.
    service_account_base64: ____BASE64_ENCODED_SERVICE_ACCOUNT____
    service_account_json: |-
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
    exclude: [bucket_a, bucket_b]
```

## Configuration

| Key | Type | Required | Description |
|:----|:-----|:---------|:------------|
| `project_id` | `string` | Yes | Google Cloud project ID. |
| `extract_blob` | `bool` | No | Extract blob metadata inside each bucket. |
| `service_account_base64` | `string` | No | Base64-encoded service account JSON. Takes precedence over `service_account_json`. |
| `service_account_json` | `string` | No | Service account JSON string. |
| `exclude` | `[]string` | No | Bucket names to exclude from extraction. |

### Notes

Leaving `service_account_base64` and `service_account_json` blank defaults to [Google Application Default Credentials](https://cloud.google.com/docs/authentication/production#automatically). Recommended when Meteor runs in the same GCP environment.

## Entities

- **Type:** `bucket`
- **URN format:** `urn:gcs:{project_id}:bucket:{bucket_name}`

### Properties

| Property | Type | Description |
|:---------|:-----|:------------|
| `properties.location` | `string` | Bucket location (e.g. `ASIA`, `US`). |
| `properties.location_type` | `string` | Location type (e.g. `region`, `multi-region`). |
| `properties.storage_type` | `string` | Storage class (e.g. `STANDARD`, `NEARLINE`). |
| `properties.versioning_enabled` | `bool` | Whether object versioning is enabled. |
| `properties.requester_pays` | `bool` | Whether requester pays is enabled. |
| `properties.retention_period_seconds` | `int64` | Retention policy period in seconds. |
| `properties.default_kms_key` | `string` | Default Cloud KMS key name for encryption. |
| `properties.log_bucket` | `string` | Bucket where access logs are written. |
| `properties.create_time` | `string` | Bucket creation timestamp (RFC 3339). |
| `properties.labels` | `map[string]string` | Bucket labels. |
| `properties.blobs` | `[]map[string]any` | List of blob metadata (when `extract_blob` is `true`). |

### Blob properties (within `properties.blobs[]`)

| Key | Type | Description |
|:----|:-----|:------------|
| `urn` | `string` | `urn:gcs:{project_id}:object:{bucket_name}/{blob_path}` |
| `name` | `string` | Blob path/name. |
| `size` | `int64` | Blob size in bytes. |
| `owner` | `string` | Owner of the blob. |
| `create_time` | `string` | Blob creation timestamp (RFC 3339). |
| `update_time` | `string` | Blob last-updated timestamp (RFC 3339). |
| `delete_time` | `string` | Blob deletion timestamp (RFC 3339). |
| `expire_time` | `string` | Blob retention expiration timestamp (RFC 3339). |

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
