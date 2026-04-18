# optimus

Extract job metadata from Optimus, including lineage and ownership.

## Usage

```yaml
source:
  name: optimus
  scope: my-optimus
  config:
    host: optimus.com:80
    max_size_in_mb: 45
```

## Configuration

| Key | Type | Required | Default | Description |
|:----|:-----|:---------|:--------|:------------|
| `host` | `string` | Yes | | Optimus gRPC host address. |
| `max_size_in_mb` | `int` | No | `45` | Maximum message size in MB for the gRPC client. |

## Entities

- **Type:** `job`
- **URN format:** `urn:optimus:{scope}:job:{project}.{namespace}.{job_name}`

### Properties

| Property | Type | Description |
|:---------|:-----|:------------|
| `properties.version` | `int32` | Job specification version. |
| `properties.project` | `string` | Optimus project name. |
| `properties.project_id` | `string` | Optimus project name (alias). |
| `properties.namespace` | `string` | Optimus namespace name. |
| `properties.owner` | `string` | Job owner email. |
| `properties.interval` | `string` | Job schedule interval. |
| `properties.depends_on_past` | `bool` | Whether the job depends on past runs. |
| `properties.task_name` | `string` | Task plugin name. |
| `properties.window_size` | `string` | Window size. |
| `properties.window_offset` | `string` | Window offset. |
| `properties.window_truncate_to` | `string` | Window truncation setting. |
| `properties.start_date` | `string` | Job start date. |
| `properties.end_date` | `string` | Job end date. |
| `properties.sql` | `string` | SQL query from `query.sql` resource (if present). |
| `properties.task` | `map[string]any` | Task details (`name`, `description`, `image`). |

## Edges

| Edge Type | Source URN | Target URN | Description |
|:----------|:-----------|:-----------|:------------|
| `lineage` | Upstream resource URN | Job URN | Upstream dependency (BigQuery or MaxCompute table). |
| `lineage` | Job URN | Downstream resource URN | Downstream destination (BigQuery or MaxCompute table). |
| `owned_by` | Job URN | `urn:user:{owner_email}` | Job owner. |

### MaxCompute support

When a job references MaxCompute resources (prefix `maxcompute://`), the extractor parses fully-qualified names in the format `maxcompute://project.schema.table` and generates URNs accordingly.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
