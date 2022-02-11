# bigtable

## Usage

```yaml
source:
  name: bigtable
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
| `resource.urn` | `project_id.instance_name.table_name` |
| `resource.name` | `table_name` |
| `resource.service` | `bigtable` |
| `properties.fields` | [Fields](#Fields) |

### Fields

| Field | Sample Value |
| :---- | :---- |
| `column_family` | `[{\"Name\":\"ts\",\"GCPolicy\":\"(age() \\u003e 90d)\"}]` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on
contributing to this module.
