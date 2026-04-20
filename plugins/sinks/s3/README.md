# S3

Write metadata records as NDJSON to an Amazon S3 or S3-compatible storage bucket.

## Usage

```yaml
sinks:
  - name: s3
    config:
      bucket_url: s3://bucket-name/optional-prefix
      region: us-east-1
      object_prefix: github-users
      access_key_id: <your-access-key>
      secret_access_key: <your-secret-key>
```

## Configuration

| Key | Type | Example | Description | |
| :-- | :--- | :------ | :---------- | :- |
| `bucket_url` | `string` | `s3://bucket-name/prefix` | S3 URL in the format `s3://<bucket>/<optional_path>` | *required* |
| `region` | `string` | `us-east-1` | AWS region for the S3 bucket | *required* |
| `object_prefix` | `string` | `github-users` | Prefix for the output object name. The final object is named `{prefix}-{timestamp}.ndjson`. If omitted, the name is `{timestamp}.ndjson`. | *optional* |
| `access_key_id` | `string` | `AKIAIOSFODNN7EXAMPLE` | AWS access key ID. If omitted, the default AWS credential chain is used. | *optional* |
| `secret_access_key` | `string` | `wJalrXUtnFEMI/K7MDENG/...` | AWS secret access key. If omitted, the default AWS credential chain is used. | *optional* |
| `endpoint` | `string` | `http://localhost:9000` | Custom endpoint URL for S3-compatible stores such as MinIO. | *optional* |

## Behavior

Each Record (Entity + Edges) is serialized as JSON and written as one line in an NDJSON object in the configured S3 bucket. A new object with a timestamped name is created on each run. If no explicit credentials are provided, the sink falls back to the default AWS credential chain (environment variables, shared config, instance profile, etc.).

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-sink) for information on contributing to this module.
