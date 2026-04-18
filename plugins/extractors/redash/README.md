# redash

Extract dashboard metadata from a Redash server.

## Usage

```yaml
source:
  name: redash
  config:
    base_url: https://redash.example.com
    api_key: t33I8i8OFnVt3t9Bjj2RXr8nCBz0xyzVZ318Zwbj
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `base_url` | `string` | Yes | Base URL of the Redash server. |
| `api_key` | `string` | Yes | API key for Redash authentication. |

## Entities

- **Entity type:** `dashboard`
- **URN format:** `urn:redash:{scope}:dashboard:{dashboard_id}`

### Properties

| Property | Type | Description |
| :------- | :--- | :---------- |
| `user_id` | `int` | ID of the dashboard owner. |
| `version` | `int` | Dashboard version number. |
| `slug` | `string` | Dashboard URL slug. |
| `is_archived` | `bool` | Whether the dashboard is archived. |
| `is_draft` | `bool` | Whether the dashboard is a draft. |
| `tags` | `[]any` | Dashboard tags. |
| `created_at` | `string` | Creation timestamp (RFC 3339). |
| `updated_at` | `string` | Last update timestamp (RFC 3339). |

## Edges

| Source | Target | Type | Description |
| :----- | :----- | :--- | :---------- |
| `dashboard` | `user` | `owned_by` | Dashboard is owned by the user who created it. |

## Contributing

Refer to the [contribution guide](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
