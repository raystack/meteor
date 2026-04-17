# Sinks

Sinks are plugins that receive [Records](metadata_models.md) and push them to destination systems. Each Record contains an Entity (with a flat `properties` map) and a list of Edges representing relationships such as ownership and lineage.

To use a sink, add a `sinks` block to your [recipe](../concepts/recipe.md):

```yaml
sinks:
  - name: compass
    config:
      host: https://compass.example.com
```

## Supported Sinks

| Sink | Description | Output |
| :--- | :--- | :--- |
| [`compass`](#compass) | Send entities and edges to Compass | Compass API (HTTP) |
| [`kafka`](#kafka) | Publish entity as protobuf to a Kafka topic | Kafka topic |
| [`file`](#file) | Write records to a local file | NDJSON or YAML file |
| [`console`](#console) | Print records to stdout | Standard output |
| [`http`](#http) | Send entity JSON to any HTTP endpoint | HTTP API |
| [`stencil`](#stencil) | Register table schemas in Stencil | Stencil API (HTTP) |
| [`gcs`](#gcs) | Write records as NDJSON to Google Cloud Storage | GCS bucket |

## Compass

Sends each Record to [Compass](https://github.com/raystack/compass) via HTTP. The entity is upserted with its properties, and lineage edges are included inline. Non-lineage edges (such as `owned_by`) are upserted separately.

```yaml
sinks:
  - name: compass
    config:
      host: https://compass.example.com
      headers:
        Compass-User-UUID: meteor@raystack.io
```

| Key | Description | Required |
| :-- | :---------- | :------- |
| `host` | Compass service hostname | Yes |
| `headers` | Additional HTTP headers (comma-separated values for multiple) | No |

## Kafka

Serializes the entity as a protobuf message and publishes it to a Kafka topic. The optional `key_path` extracts a field from the entity to use as the Kafka message key.

```yaml
sinks:
  - name: kafka
    config:
      brokers: localhost:9092
      topic: metadata-topic
      key_path: .Urn
```

| Key | Description | Required |
| :-- | :---------- | :------- |
| `brokers` | Comma-separated list of Kafka broker addresses | Yes |
| `topic` | Kafka topic to publish messages to | Yes |
| `key_path` | Field path on the entity proto to use as the message key (e.g. `.Urn`) | No |

## File

Writes records to a local file in NDJSON or YAML format. Each record is serialized as JSON (entity + edges).

```yaml
sinks:
  - name: file
    config:
      path: ./output.ndjson
      format: ndjson
      overwrite: true
```

| Key | Description | Required |
| :-- | :---------- | :------- |
| `path` | Output file path | Yes |
| `format` | Output format: `ndjson` or `yaml` | Yes |
| `overwrite` | Overwrite existing file (default `true`) | No |

## Console

Prints each record as JSON to stdout. Useful for debugging recipes.

```yaml
sinks:
  - name: console
```

No configuration required.

## HTTP

Sends the entity as JSON to an arbitrary HTTP endpoint. The URL supports Go template variables from the entity (e.g. `{{ .Type }}`, `{{ .Urn }}`). An optional Tengo script can transform the payload before sending.

```yaml
sinks:
  - name: http
    config:
      url: https://example.com/metadata/{{ .Type }}
      method: PUT
      success_code: 200
      headers:
        Authorization: Bearer token
```

| Key | Description | Required |
| :-- | :---------- | :------- |
| `url` | Target URL (supports Go template variables) | Yes |
| `method` | HTTP method (`GET`, `POST`, `PUT`, `PATCH`, etc.) | Yes |
| `success_code` | Expected HTTP status code for success (default `200`) | No |
| `headers` | Additional HTTP headers | No |
| `script.engine` | Script engine for payload transformation (`tengo`) | No |
| `script.source` | Tengo script source code | No |

## Stencil

Registers table column schemas in [Stencil](https://github.com/raystack/stencil) as JSON Schema or Avro. Only entities with a `columns` field in their properties are processed. Column types from BigQuery and PostgreSQL are automatically mapped to the target schema format.

```yaml
sinks:
  - name: stencil
    config:
      host: https://stencil.example.com
      namespace_id: myNamespace
      format: json
```

| Key | Description | Required |
| :-- | :---------- | :------- |
| `host` | Stencil service hostname | Yes |
| `namespace_id` | Stencil namespace to register schemas under | Yes |
| `format` | Schema format: `json` or `avro` (default `json`) | No |

## GCS

Writes records as NDJSON to a Google Cloud Storage bucket. Each record is serialized as a JSON line. The output object is named with an optional prefix and a timestamp.

```yaml
sinks:
  - name: gcs
    config:
      project_id: my-gcp-project
      url: gcs://bucket_name/target_folder
      object_prefix: github-users
      service_account_base64: <base64-encoded-service-account-key>
```

| Key | Description | Required |
| :-- | :---------- | :------- |
| `project_id` | GCP project ID | Yes |
| `url` | GCS destination in the form `gcs://bucket/path` | Yes |
| `object_prefix` | Prefix for the output object name | No |
| `service_account_base64` | Base64-encoded service account JSON key | No* |
| `service_account_json` | Service account JSON key as a string | No* |

*One of `service_account_base64` or `service_account_json` is required.
