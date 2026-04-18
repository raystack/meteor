# superset

Extract dashboard metadata from an Apache Superset server.

## Usage

```yaml
source:
  name: superset
  config:
    username: meteor_tester
    password: meteor_pass_1234
    host: http://localhost:3000
    provider: db
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `username` | `string` | Yes | Username for Superset authentication. |
| `password` | `string` | Yes | Password for Superset authentication. |
| `host` | `string` | Yes | URL of the Superset server. |
| `provider` | `string` | Yes | Authentication provider (e.g. `db`). |

## Entities

- **Entity type:** `dashboard`
- **URN format:** `urn:superset:{scope}:dashboard:{dashboard_id}`

### Properties

| Property | Type | Description |
| :------- | :--- | :---------- |
| `url` | `string` | Dashboard URL. |
| `slug` | `string` | Dashboard slug. |
| `published` | `bool` | Whether the dashboard is published. |
| `changed_by` | `string` | Name of the user who last modified the dashboard. |
| `charts` | `[]object` | List of chart objects (see below). |

### Chart sub-fields

| Field | Type | Description |
| :---- | :--- | :---------- |
| `urn` | `string` | Chart URN (`urn:superset:{scope}:chart:{slice_id}`). |
| `name` | `string` | Chart/slice name. |
| `source` | `string` | Always `superset`. |
| `description` | `string` | Chart description (if set). |
| `url` | `string` | Direct URL to the chart. |
| `data_source` | `string` | Datasource backing the chart. |
| `dashboard_urn` | `string` | Parent dashboard reference (`dashboard:{dashboard_id}`). |

## Edges

| Source | Target | Type | Description |
| :----- | :----- | :--- | :---------- |
| `dashboard` | `user` | `owned_by` | Dashboard is owned by a user. |

## Contributing

Refer to the [contribution guide](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
