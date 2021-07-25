# bigtable

## Usage

```yaml
source:
  type: bigtable
  config:
    project_id: google-project-id
```

## Inputs

| Key        | Value  | Example    | Description         | required |
|------------|--------|------------|---------------------|----------|
| project_id | string | my-project | BigTable Project ID | true     |

### *Notes*

You will have to set the env var `GOOGLE_APPLICATION_CREDENTIALS` with value as path of the service account json file.

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `urn` | `project_id.instance_name.table_name` |
| `name` | `table_name` |
| `source` | `bigtable` |
| `custom` | [CustomProperties](#CustomProperties) |

### CustomProperties

| Field | Sample Value |
| :---- | :---- |
| `column_family` | `[{\"Name\":\"ts\",\"GCPolicy\":\"(age() \\u003e 90d)\"}]` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on
contributing to this module.
