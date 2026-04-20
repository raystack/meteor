# GCS

Write metadata records as NDJSON to a Google Cloud Storage bucket.

## Usage

```yaml
sinks:
  - name: gcs
    config:
      project_id: google-project-id
      url: gcs://bucket_name/target_folder
      object_prefix: github-users
      service_account_base64: <base64 encoded service account key>
```

## Configuration

| Key | Type | Example | Description | |
| :-- | :--- | :------ | :---------- | :- |
| `project_id` | `string` | `google-project-id` | Google Cloud project ID | *required* |
| `url` | `string` | `gcs://bucket_name/target_folder` | GCS URL in the format `gcs://<bucket>/<optional_path>` | *required* |
| `object_prefix` | `string` | `github-users` | Prefix for the output object name. The final object is named `{prefix}-{timestamp}.ndjson`. If omitted, the name is `{timestamp}.ndjson`. | *optional* |
| `service_account_base64` | `string` | `ewog....fQo=` | Service account key as a base64-encoded string. Takes precedence over `service_account_json`. | *optional* |
| `service_account_json` | `string` | `{"private_key": ...}` | Service account key as a JSON string. Either this or `service_account_base64` must be provided. | *optional* |

## Behavior

Each Record (Entity + Edges) is serialized as JSON and written as one line in an NDJSON object in the configured GCS bucket. A new object with a timestamped name is created on each run.

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.mdx#adding-a-new-sink) for information on contributing to this module.
