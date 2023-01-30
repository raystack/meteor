# superset

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

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `username` | `string` | `meteor_tester` | Username to access the superset| *required* |
| `password` | `string` | `meteor_pass_1234` | Password for the superset | *required* |
| `host` | `string` | `http://localhost:4002` | Host at which superset is running | *required* |
| `provider` | `string` | `db` | Provider for the superset | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `superset.dashboard_name` |
| `resource.name` | `dashboard_name` |
| `resource.service` | `superset` |
| `resource.url` | `dashboard_url` |

### Chart

| Field | Sample Value |
| :---- | :---- |
| `name` | `chart_name` |
| `dashboard_source` | `superset` |
| `description` | `chart_description` |
| `url` | `chart_url` |
| `datasource` | `chart_datasource` |
| `dashboard_urn` | `dashboard:dashboard_id` |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
