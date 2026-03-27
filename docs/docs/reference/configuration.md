# Configuration

This page contains references for all the application configurations for Meteor.

## Table of Contents

- [Server](#server)
- [Recipe](#recipe)
- [Log](#log)
- [Extractor Configuration](#extractor-configuration)
- [Sink Configuration](#sink-configuration)
- [Processor Configuration](#processor-configuration)

## Server

Configuration for the Meteor server when running in server mode.

### `PORT`

- Example value: `8080`
- Type: `optional`
- Default: `3000`
- Port to listen on.

## Recipe

### `RECIPE_STORAGE_URL`

- Example value: `s3://my-bucket?region=us-west-1`
- Type: `optional`
- Default: `mem://`
- Object storage URL to persist recipes. Supports GCS (`gs://`), S3 (`s3://`), local filesystem (`file:///path`), and in-memory (`mem://`).

## Log

### `LOG_LEVEL`

- Example value: `debug`
- Type: `optional`
- Default: `info`
- Controls log verbosity. Supported values: `debug`, `info`, `warn`, `error`.

## Environment Variables

Meteor reads environment variables with the `METEOR_` prefix as template data for recipes. This allows you to inject credentials and configuration without hardcoding them.

For example, setting `METEOR_BIGQUERY_PROJECT_ID=my-project` makes `bigquery_project_id` available in recipes:

```yaml
source:
  name: bigquery
  config:
    project_id: "{{ .bigquery_project_id }}"
```

See [Dynamic recipe value](../concepts/recipe.md#dynamic-recipe-value) for details.

## Extractor Configuration

Each extractor has its own configuration schema. Common patterns:

### Database Extractors

Most database extractors (postgres, mysql, mssql, mariadb, oracle, clickhouse, redshift, snowflake, presto) accept:

| Key | Description |
| :-- | :---------- |
| `connection_url` | Database connection string (format varies by database) |
| `exclude` | Map of databases/schemas/tables to exclude from extraction |

### Cloud Service Extractors

Extractors for GCP services (bigquery, bigtable, gcs) commonly accept:

| Key | Description |
| :-- | :---------- |
| `project_id` | GCP project ID |
| `service_account_base64` | Base64-encoded service account JSON key |
| `service_account_json` | Service account JSON key (inline) |

### API-based Extractors

Extractors for BI tools (tableau, metabase, superset, grafana, redash) commonly accept:

| Key | Description |
| :-- | :---------- |
| `host` | Service host URL |
| `username` | Authentication username |
| `password` | Authentication password |

For complete configuration details of each extractor, refer to the individual README files linked from the [extractors reference](extractors.md).

## Sink Configuration

Each sink has its own configuration. Common patterns:

| Sink | Key Config Fields |
| :--- | :---------------- |
| `compass` | `host`, `type`, `mapping` |
| `kafka` | `brokers`, `topic`, `key_path` |
| `http` | `method`, `url`, `headers`, `success_code` |
| `gcs` | `project_id`, `url`, `object_prefix`, `service_account_base64` |
| `file` | `path`, `format` |
| `stencil` | `host`, `namespace_id`, `schema_id`, `format` |
| `frontier` | `host`, `headers` |

For complete configuration details, refer to the [sinks reference](sinks.md).

## Processor Configuration

| Processor | Key Config Fields |
| :-------- | :---------------- |
| `enrich` | `attributes` — key-value map to merge into asset attributes |
| `labels` | `labels` — key-value map to append to asset labels |
| `script` | `engine`, `script` — Tengo script for custom transformation |

For complete configuration details, refer to the [processors reference](processors.md).
