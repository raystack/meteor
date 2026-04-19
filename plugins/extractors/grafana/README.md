# Grafana

Extract dashboard and datasource metadata from a Grafana server.

## Usage

```yaml
source:
  name: grafana
  config:
    base_url: http://localhost:3000
    api_key: Bearer qweruqwryqwLKJ
    # extract specifies which entity types to extract.
    # Defaults to all: ["dashboards", "datasources"]
    extract:
      - dashboards
      - datasources
    exclude:
      dashboards:
        - dashboard_uid_1
        - dashboard_uid_2
      panels:
        - dashboard_uid_3.panel_id_1
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `base_url` | `string` | Yes | URL of the Grafana server. |
| `api_key` | `string` | Yes | API key to access the Grafana API. |
| `extract` | `[]string` | No | Entity types to extract. Defaults to `["dashboards", "datasources"]`. |
| `exclude.dashboards` | `[]string` | No | Dashboard UIDs to exclude from extraction. |
| `exclude.panels` | `[]string` | No | Panel IDs to exclude, in the format `{dashboard_uid}.{panel_id}`. |

## Entities

### Dashboard

- **Entity type:** `dashboard`
- **URN format:** `urn:grafana:{scope}:dashboard:{uid}`

#### Properties

| Property | Type | Description |
| :------- | :--- | :---------- |
| `url` | `string` | Full URL to the dashboard. |
| `description` | `string` | Dashboard description (if set). |
| `folder` | `string` | Folder name containing the dashboard. |
| `created_by` | `string` | User who created the dashboard. |
| `updated_by` | `string` | User who last updated the dashboard. |
| `tags` | `[]string` | Dashboard tags. |
| `charts` | `[]object` | List of panel/chart objects (see below). |

#### Chart sub-fields

| Field | Type | Description |
| :---- | :--- | :---------- |
| `urn` | `string` | Panel URN (`urn:grafana:{scope}:panel:{dashboard_uid}.{panel_id}`). |
| `name` | `string` | Panel title. |
| `type` | `string` | Panel visualization type (e.g. `table`, `graph`). |
| `source` | `string` | Always `grafana`. |
| `description` | `string` | Panel description (if set). |
| `url` | `string` | Direct URL to the panel. |
| `data_source` | `string` | Data source type (if set). |
| `raw_query` | `string` | Raw SQL query from the first target (if set). |
| `dashboard_urn` | `string` | Parent dashboard URN. |
| `dashboard_source` | `string` | Always `grafana`. |

### Datasource

- **Entity type:** `datasource`
- **URN format:** `urn:grafana:{scope}:datasource:{uid}`

#### Properties

| Property | Type | Description |
| :------- | :--- | :---------- |
| `type` | `string` | Datasource type (e.g. `postgres`, `prometheus`). |
| `url` | `string` | Connection URL. |
| `database` | `string` | Database name (if applicable). |
| `is_default` | `bool` | Whether this is the default datasource. |

## Edges

| Source | Target | Type | Description |
| :----- | :----- | :--- | :---------- |
| `dashboard` | `datasource` | `derived_from` | Dashboard panels consume data from this datasource. |

## Contributing

Refer to the [contribution guide](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
