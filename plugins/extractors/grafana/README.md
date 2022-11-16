# grafana

## Usage

```yaml
source:
  name: grafana
  config:
    base_url: grafana_server
    api_key: your_api_key
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `base_url` | `string` | `http://localhost:3000` | URL of the Grafana server | *required* |
| `api_key` | `string` | `Bearer qweruqwryqwLKJ` | API key to access Grafana API | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `grafana.HzK8qNW7z` |
| `resource.name` | `new-dashboard-copy` |
| `resource.service` | `grafana` |
| `resource.url` | `http://localhost:3000/d/HzK8qNW7z/new-dashboard-copy` |
| `charts` | [][chart](#chart) |

### Chart

| Field | Sample Value |
| :---- | :---- |
| `urn` | `5WsKOvW7z.4` |
| `name` | `Panel Random` |
| `type` | `table` |
| `source` | `grafana` |
| `description` | `random description for this panel` |
| `url` | `http://localhost:3000/d/5WsKOvW7z/test-dashboard-updated?viewPanel=4` |
| `data_source` | `postgres` |
| `raw_query` | `SELECT\n  urn,\n  created_at AS \"time\"\nFROM resources\nORDER BY 1` |
| `dashboard_urn` | `grafana.5WsKOvW7z` |
| `dashboard_source` | `grafana` |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
