# PagerDuty

Extract service and incident metadata from PagerDuty using the PagerDuty REST API v2.

## Usage

```yaml
source:
  name: pagerduty
  scope: my-pagerduty
  config:
    api_key: your-pagerduty-api-key
    exclude:
      - PABC123
    incident_days: 30
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `api_key` | `string` | Yes | PagerDuty API key for authentication. |
| `exclude` | `[]string` | No | Service IDs to exclude from extraction. |
| `incident_days` | `int` | No | Number of days to look back for incidents. Defaults to `30`. |

## Entities

The extractor emits two entity types and their relationships as edges.

### Entity: `service`

| Field | Sample Value |
| :---- | :----------- |
| `urn` | `urn:pagerduty:my-pagerduty:service:PSERVICE1` |
| `name` | `Payment Gateway` |
| `description` | `Handles payment processing` |
| `properties.status` | `active` |
| `properties.description` | `Handles payment processing` |
| `properties.created_at` | `2024-01-15T10:30:00Z` |
| `properties.updated_at` | `2024-03-20T14:15:00Z` |
| `properties.html_url` | `https://mycompany.pagerduty.com/services/PSERVICE1` |
| `properties.escalation_policy_id` | `PPOLICY1` |
| `properties.team_ids` | `PTEAM1,PTEAM2` |
| `properties.alert_creation` | `create_alerts_and_incidents` |
| `properties.incident_urgency_rule` | `constant` |

### Entity: `incident`

| Field | Sample Value |
| :---- | :----------- |
| `urn` | `urn:pagerduty:my-pagerduty:incident:PINC001` |
| `name` | `High CPU on payment-gateway-01` |
| `properties.status` | `resolved` |
| `properties.urgency` | `high` |
| `properties.priority` | `P1` |
| `properties.created_at` | `2024-03-18T02:15:00Z` |
| `properties.resolved_at` | `2024-03-18T03:45:00Z` |
| `properties.html_url` | `https://mycompany.pagerduty.com/incidents/PINC001` |
| `properties.incident_number` | `42` |
| `properties.title` | `High CPU on payment-gateway-01` |

### Edges

| Type | Source | Target | Description |
| :--- | :----- | :----- | :---------- |
| `owned_by` | `service` | `team` | Service is owned by a team |
| `belongs_to` | `incident` | `service` | Incident belongs to a service |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.mdx#adding-a-new-extractor) for information on contributing to this module.
