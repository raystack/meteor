# Stencil

Register entity column schemas with a Stencil schema registry in JSON Schema or Avro format.

## Usage

```yaml
sinks:
  - name: stencil
    config:
      host: https://stencil.com
      namespace_id: my-namespace
      format: json
```

## Configuration

| Key | Type | Example | Description | |
| :-- | :--- | :------ | :---------- | :- |
| `host` | `string` | `https://stencil.com` | Hostname of the Stencil service | *required* |
| `namespace_id` | `string` | `my-namespace` | Namespace in which schemas are created | *required* |
| `format` | `string` | `json` | Schema format: `json` or `avro`. Default: `json`. | *optional* |

## Behavior

For each Record whose Entity properties contain a `columns` list, the sink builds a schema (JSON Schema or Avro) from the column metadata (name, data type, nullability, description). The schema is posted to `POST /v1beta1/namespaces/{namespace_id}/schemas/{schema_id}`, where `schema_id` is derived from the Entity URN.

Entities without columns are skipped. Type mapping from source-specific column types (BigQuery, Postgres) to JSON Schema / Avro types is handled automatically. Server errors (5xx) are returned as retryable errors.

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.mdx#adding-a-new-sink) for information on contributing to this module.
