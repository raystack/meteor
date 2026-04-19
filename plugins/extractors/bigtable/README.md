# Google Cloud Bigtable

Extract table metadata from Google Cloud Bigtable instances.

## Usage

```yaml
source:
  name: bigtable
  config:
    project_id: google-project-id
    service_account_base64: _base64_encoded_service_account_
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `project_id` | `string` | Yes | GCP project ID containing Bigtable instances. |
| `service_account_base64` | `string` | No | Base64-encoded service account JSON. If omitted, the `GOOGLE_APPLICATION_CREDENTIALS` environment variable must point to a valid service account file. |

## Entities

- Entity type: `table`
- URN format: `urn:bigtable:{project_id}:table:{instance}.{table}`

| Property | Type | Description |
| :------- | :--- | :---------- |
| `properties.column_family` | `string` | JSON-encoded list of column family info objects (name and GC policy). |

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
