# application_yaml

Extract application metadata from a YAML file, including lineage and ownership.

## Usage

```yaml
source:
  name: application_yaml
  scope: applications-stg
  config:
    file: "./path/to/meteor.app.yaml"
    env_prefix: CI
```

## Configuration

| Key | Type | Required | Default | Description |
|:----|:-----|:---------|:--------|:------------|
| `file` | `string` | Yes | | Path to the application YAML file. |
| `env_prefix` | `string` | No | `CI` | Prefix for environment variables injected as template variables (prefix is trimmed). |

### Application YAML format

```yaml
name: "order-manager"             # required
id: "0adf3214-676c-..."           # required
team:
  id: "team-123"
  name: "Order Team"
  email: "order-team@example.com"
description: "Order management service"
url: "https://github.com/mycompany/order-manager"
version: "d6ec883"
inputs:                            # upstream lineage URNs
  - urn:bigquery:bq-raw:table:project.dataset.table
  - urn:kafka:my-kafka:topic:my-topic
outputs:                           # downstream lineage URNs
  - urn:kafka:my-kafka:topic:output-topic
create_time: "2006-01-02T15:04:05Z"
update_time: "2006-01-02T15:04:05Z"
labels:
  team: "Booking Experience"
```

Environment variables with the configured prefix (default `CI`) are available as Go template variables with the prefix stripped. For example, `CI_PROJECT_NAME` becomes `{{.project_name}}`.

## Entities

- **Type:** `application`
- **URN format:** `urn:application_yaml:{scope}:application:{name}`

### Properties

| Property | Type | Description |
|:---------|:-----|:------------|
| `properties.id` | `string` | Application ID. |
| `properties.version` | `string` | Application version. |
| `properties.url` | `string` | Application URL. |
| `properties.create_time` | `string` | Creation timestamp (RFC 3339). |
| `properties.update_time` | `string` | Last update timestamp (RFC 3339). |
| `properties.labels` | `map[string]string` | Key-value labels. |

## Edges

| Edge Type | Source URN | Target URN | Description |
|:----------|:-----------|:-----------|:------------|
| `owned_by` | Application URN | `urn:user:{team.email}` (or `urn:user:{team.id}`) | Team ownership. Emitted when `team.id` is set. |
| `derived_from` | Input URN | Application URN | Upstream dependency from `inputs[]`. |
| `generates` | Application URN | Output URN | Downstream dependency from `outputs[]`. |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
