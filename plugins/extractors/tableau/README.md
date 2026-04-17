# tableau

Extract dashboard (workbook) metadata from a Tableau server.

> **Note:** You must enable the [Metadata API](https://help.tableau.com/current/api/metadata_api/en-us/) on your Tableau server for this extractor to work.

## Usage

```yaml
source:
  name: tableau
  config:
    host: https://server.tableau.com
    version: "3.12"
    username: meteor_user
    password: xxxxxxxxxx
    sitename: testdev550928
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `host` | `string` | Yes | URL of the Tableau server. |
| `version` | `string` | Yes | [Tableau REST API version](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_versions.htm) (e.g. `3.12`). |
| `username` | `string` | No (required without `auth_token`) | Username or email for authentication. |
| `password` | `string` | No (required with `username`) | Password for authentication. |
| `auth_token` | `string` | No (required without `username`) | Personal access token for authentication. |
| `site_id` | `string` | No (required without `username`) | Site ID, used with `auth_token`. |
| `sitename` | `string` | No | Tableau site name. Defaults to the default site if empty. |

## Entities

- **Entity type:** `dashboard`
- **URN format:** `urn:tableau:{scope}:workbook:{workbook_id}`

### Properties

| Property | Type | Description |
| :------- | :--- | :---------- |
| `properties.id` | `string` | Workbook ID. |
| `properties.name` | `string` | Workbook name. |
| `properties.project_name` | `string` | Project the workbook belongs to. |
| `properties.uri` | `string` | Workbook URI on the Tableau server. |
| `properties.owner_id` | `string` | Owner user ID. |
| `properties.owner_name` | `string` | Owner display name. |
| `properties.owner_email` | `string` | Owner email address. |
| `properties.create_time` | `string` | Creation timestamp (RFC 3339). |
| `properties.update_time` | `string` | Last update timestamp (RFC 3339). |
| `properties.charts` | `[]object` | List of sheet/chart objects (see below). |

### Chart sub-fields

| Field | Type | Description |
| :---- | :--- | :---------- |
| `urn` | `string` | Sheet URN (`urn:tableau:{scope}:sheet:{sheet_id}`). |
| `name` | `string` | Sheet name. |
| `id` | `string` | Sheet ID. |
| `source` | `string` | Always `tableau`. |
| `dashboard_urn` | `string` | Parent workbook URN. |
| `create_time` | `string` | Sheet creation timestamp (if available). |
| `update_time` | `string` | Sheet last update timestamp (if available). |

## Edges

| Type | Source URN | Target URN | Description |
| :--- | :--------- | :--------- | :---------- |
| `lineage` | Upstream table URN | Dashboard URN | One edge per upstream table connected to the workbook. Supports DatabaseServer, CloudFile, File, and WebDataConnector sources. MaxCompute JDBC connections are also detected automatically. |
| `owned_by` | Dashboard URN | `urn:user:{owner_email}` | Ownership edge linking the workbook to its owner. |

## Contributing

Refer to the [contribution guide](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
