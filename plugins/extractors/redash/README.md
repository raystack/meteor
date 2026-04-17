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
| `properties.user_id` | `int` | ID of the dashboard owner. |
| `properties.version` | `int` | Dashboard version number. |
| `properties.slug` | `string` | Dashboard URL slug. |

> **Note:** The Redash public API does not expose chart/widget details for dashboards, so `properties.charts` is not emitted.

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guide](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
