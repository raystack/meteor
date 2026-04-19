# Metabase

Extract dashboard metadata from a Metabase server.

## Usage

```yaml
source:
  name: metabase
  config:
    host: http://localhost:3000
    instance_label: my-metabase
    username: meteor_tester
    password: meteor_pass_1234
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `host` | `string` | Yes | URL of the Metabase server. |
| `instance_label` | `string` | Yes | Instance alias used as part of the URN. |
| `username` | `string` | Yes (unless `session_id` is set) | Username or email for authentication. |
| `password` | `string` | No | Password for authentication. |
| `session_id` | `string` | No | Existing Metabase session ID. When set, `username` and `password` are ignored. |

## Entities

- **Entity type:** `dashboard`
- **URN format:** `urn:metabase:{scope}:collection:{dashboard_id}`

### Properties

| Property | Type | Description |
| :------- | :--- | :---------- |
| `properties.id` | `int` | Dashboard ID. |
| `properties.collection_id` | `int` | Collection ID the dashboard belongs to. |
| `properties.creator_id` | `int` | ID of the dashboard creator. |
| `properties.create_time` | `string` | Creation timestamp (RFC 3339). |
| `properties.update_time` | `string` | Last update timestamp (RFC 3339). |
| `properties.charts` | `[]object` | List of card/chart objects (see below). |

### Chart sub-fields

| Field | Type | Description |
| :---- | :--- | :---------- |
| `urn` | `string` | Card URN (`metabase::{instance_label}/card/{card_id}`). |
| `name` | `string` | Card name. |
| `source` | `string` | Always `metabase`. |
| `description` | `string` | Card description (if set). |
| `dashboard_urn` | `string` | Parent dashboard URN. |
| `id` | `int` | Card ID. |
| `collection_id` | `int` | Collection ID. |
| `creator_id` | `int` | Creator ID. |
| `database_id` | `int` | Database ID backing the card. |
| `table_id` | `int` | Table ID backing the card. |
| `query_average_duration` | `int` | Average query duration in ms. |
| `display` | `string` | Card display type. |
| `archived` | `bool` | Whether the card is archived. |
| `upstreams` | `[]string` | Upstream table URNs (if resolved). |

## Edges

| Type | Source URN | Target URN | Description |
| :--- | :--------- | :--------- | :---------- |
| `derived_from` | Upstream table URN | Dashboard URN | One edge per unique upstream table resolved from cards. Supports postgres, mysql, bigquery, and h2 databases. |
| `owned_by` | Dashboard URN | User URN | Dashboard is owned by the user who created it. |

## Contributing

Refer to the [contribution guide](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
