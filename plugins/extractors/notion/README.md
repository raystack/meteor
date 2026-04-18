# Notion

Extract page and database metadata from a Notion workspace using the Notion API.

## Usage

```yaml
source:
  name: notion
  scope: my-workspace
  config:
    token: ntn_your_integration_token
    extract:
      - pages
      - databases
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `token` | `string` | Yes | Notion internal integration token. |
| `base_url` | `string` | No | Override Notion API base URL. Defaults to `https://api.notion.com`. |
| `extract` | `[]string` | No | Entity types to extract. Defaults to all: `pages`, `databases`. |

## Entities

The extractor emits `document` entities for both pages and databases.

### Entity: `document` (page)

| Field | Sample Value |
| :---- | :----------- |
| `urn` | `urn:notion:my-workspace:document:abc123-def456` |
| `name` | `Data Pipeline Architecture` |
| `properties.page_id` | `abc123-def456` |
| `properties.created_at` | `2024-01-15T10:30:00Z` |
| `properties.updated_at` | `2024-03-20T14:15:00Z` |
| `properties.created_by` | `Alice` |
| `properties.last_edited_by` | `Bob` |
| `properties.web_url` | `https://www.notion.so/Data-Pipeline-abc123` |
| `properties.archived` | `false` |

### Entity: `document` (database)

| Field | Sample Value |
| :---- | :----------- |
| `urn` | `urn:notion:my-workspace:document:db-789` |
| `name` | `Project Tracker` |
| `description` | `Track all engineering projects` |
| `properties.database_id` | `db-789` |
| `properties.created_at` | `2024-01-10T09:00:00Z` |
| `properties.updated_at` | `2024-03-18T16:00:00Z` |
| `properties.created_by` | `Alice` |
| `properties.columns` | `["Name", "Status", "Priority"]` |
| `properties.web_url` | `https://www.notion.so/db-789` |

### Edges

| Type | Source | Target | Description |
| :--- | :----- | :----- | :---------- |
| `child_of` | `document` | `document` | Page is a child of another page |
| `belongs_to` | `document` | `document` | Page belongs to a database |
| `owned_by` | `document` | `user` | Page/database is owned by its creator |
| `documented_by` | `document` | any | Page references a data asset via URN in its content |

### URN Reference Detection

The extractor reads page block content and scans for URN patterns (`urn:service:scope:type:id`), emitting `documented_by` edges to link documentation to data assets.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
