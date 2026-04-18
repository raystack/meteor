# Confluence

Extract page metadata and relationships from Confluence spaces using the Confluence REST API v2.

## Usage

```yaml
source:
  name: confluence
  scope: my-confluence
  config:
    base_url: https://mycompany.atlassian.net/wiki
    username: user@company.com
    token: your-api-token
    spaces:
      - ENG
      - DATA
    exclude:
      - ARCHIVE
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `base_url` | `string` | Yes | Confluence base URL (e.g. `https://mycompany.atlassian.net/wiki`). |
| `username` | `string` | Yes | Atlassian account email for API authentication. |
| `token` | `string` | Yes | Atlassian API token. |
| `spaces` | `[]string` | No | Space keys to extract. Defaults to all spaces. |
| `exclude` | `[]string` | No | Space keys to exclude from extraction. |

## Entities

The extractor emits two entity types and their relationships as edges.

### Entity: `space`

| Field | Sample Value |
| :---- | :----------- |
| `urn` | `urn:confluence:my-confluence:space:ENG` |
| `name` | `Engineering` |
| `description` | `Engineering team documentation` |
| `properties.space_key` | `ENG` |
| `properties.space_type` | `global` |
| `properties.status` | `current` |
| `properties.web_url` | `https://mycompany.atlassian.net/wiki/spaces/ENG` |

### Entity: `document`

| Field | Sample Value |
| :---- | :----------- |
| `urn` | `urn:confluence:my-confluence:document:12345` |
| `name` | `Data Pipeline Architecture` |
| `properties.page_id` | `12345` |
| `properties.space_key` | `ENG` |
| `properties.status` | `current` |
| `properties.version` | `5` |
| `properties.labels` | `["architecture", "data"]` |
| `properties.created_at` | `2024-01-15T10:30:00Z` |
| `properties.updated_at` | `2024-03-20T14:15:00Z` |
| `properties.web_url` | `https://mycompany.atlassian.net/wiki/spaces/ENG/pages/12345` |

### Edges

| Type | Source | Target | Description |
| :--- | :----- | :----- | :---------- |
| `belongs_to` | `document` | `space` | Page belongs to a space |
| `child_of` | `document` | `document` | Page is a child of another page |
| `owned_by` | `document` | `user` | Page is owned by its author |
| `documented_by` | `document` | any | Page references a data asset via URN in its content |

### URN Reference Detection

The extractor scans page content for URN patterns (`urn:service:scope:type:id`) and emits `documented_by` edges linking the page to referenced data assets. This enables connecting business documentation to technical metadata.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
