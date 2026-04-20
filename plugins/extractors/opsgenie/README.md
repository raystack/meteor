# OpsGenie

Extract service and incident metadata from OpsGenie using the OpsGenie REST API.

## Usage

```yaml
source:
  name: opsgenie
  scope: my-opsgenie
  config:
    api_key: your-api-key
    base_url: https://api.opsgenie.com
    exclude:
      - service-id-to-skip
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `api_key` | `string` | Yes | OpsGenie API key for authentication. |
| `base_url` | `string` | No | OpsGenie API base URL. Defaults to `https://api.opsgenie.com`. |
| `exclude` | `[]string` | No | Service IDs to exclude from extraction. |

> **Note:** For EU instances, set `base_url: https://api.eu.opsgenie.com`.

## Entities

The extractor emits two entity types and their relationships as edges.

### Entity: `service`

| Field | Sample Value |
| :---- | :----------- |
| `urn` | `urn:opsgenie:my-opsgenie:service:abc-123` |
| `name` | `Payment Service` |
| `description` | `Handles payment processing` |
| `properties.description` | `Handles payment processing` |
| `properties.team_id` | `team-456` |
| `properties.html_url` | `https://app.opsgenie.com/service/abc-123` |

### Entity: `incident`

| Field | Sample Value |
| :---- | :----------- |
| `urn` | `urn:opsgenie:my-opsgenie:incident:inc-789` |
| `name` | `Database connection timeout` |
| `properties.status` | `open` |
| `properties.priority` | `P1` |
| `properties.created_at` | `2024-01-15T10:30:00Z` |
| `properties.resolved_at` | `2024-01-15T11:00:00Z` |
| `properties.html_url` | `https://app.opsgenie.com/incident/detail/inc-789` |
| `properties.message` | `Database connection timeout` |
| `properties.tags` | `["database", "critical"]` |
| `properties.owner_id` | `team-456` |

### Edges

| Type | Source | Target | Description |
| :--- | :----- | :----- | :---------- |
| `owned_by` | `service` | `team` | Service is owned by a team |
| `belongs_to` | `incident` | `service` | Incident impacts a service |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.mdx#adding-a-new-extractor) for information on contributing to this module.
