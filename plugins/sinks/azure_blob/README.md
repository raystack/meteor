# Azure Blob

Write metadata records as NDJSON to an Azure Blob Storage container.

## Usage

```yaml
sinks:
  - name: azure_blob
    config:
      storage_account_url: https://myaccount.blob.core.windows.net
      container_name: my-container
      object_prefix: github-users
      account_key: <your-account-key>
```

## Configuration

| Key | Type | Example | Description | |
| :-- | :--- | :------ | :---------- | :- |
| `storage_account_url` | `string` | `https://myaccount.blob.core.windows.net` | Azure Storage account URL | *required* |
| `container_name` | `string` | `my-container` | Name of the blob container to write to | *required* |
| `object_prefix` | `string` | `github-users` | Prefix for the output blob name. The final blob is named `{prefix}-{timestamp}.ndjson`. If omitted, the name is `{timestamp}.ndjson`. | *optional* |
| `account_key` | `string` | `abc123...` | Azure Storage shared key. Either this or `connection_string` must be provided. | *optional* |
| `connection_string` | `string` | `DefaultEndpointsProtocol=https;...` | Azure Storage connection string. Takes precedence over `account_key`. | *optional* |

## Behavior

Each Record (Entity + Edges) is serialized as JSON and written as one line in an NDJSON blob in the configured Azure Blob Storage container. A new blob with a timestamped name is created on each run.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-sink) for information on contributing to this module.
