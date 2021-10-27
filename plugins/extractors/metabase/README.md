# metabase

## Usage

```yaml
source:
  type: metabase
  config:
    host: http://localhost:3000
    username: meteor_tester
    password: meteor_pass_1234
    label: my-metabase
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `host` | `string` | `http://localhost:4002` | The host at which metabase is running | *required* |
| `username` | `string` | `meteor_tester` | Username/email to access the metabase| *required* |
| `password` | `string` | `meteor_pass_1234` | Password for the metabase | *required* |
| `label` | `string` | `meteor_pass_1234` | Label for your Metabase instance, this will be used as part of dashboard's URN | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `metabase.dashboard_name` |
| `resource.name` | `dashboard_name` |
| `resource.service` | `metabase` |
| `description` | `table description` |
| `charts` | [][Chart](#chart) |

### Chart

| Field | Sample Value |
| :---- | :---- |
| `urn` | `metabase.dashboard_name.card_name` |
| `source` | `metabase` |
| `dashboard_urn` | `metabase.dashboard_name` |
| `dashboard_source` | `metabase` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
