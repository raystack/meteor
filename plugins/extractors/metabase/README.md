# metabase

## Usage

```yaml
source:
  name: metabase
  config:
    host: http://localhost:3000
    instance_label: my-metabase
    username: meteor_tester
    password: meteor_pass_1234
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `host` | `string` | `http://localhost:4002` | The host at which metabase is running | *required* |
| `instance_label` | `string` | `my-metabase` | Instance alias, the value will be used as part of the urn component | *required* |
| `username` | `string` | `meteor_tester` | Username/email to access the metabase| *optional* |
| `password` | `string` | `meteor_pass_1234` | Password for the metabase | *optional* |
| `session_id` | `string` | `meteor_pass_1234` | Use existing session ID from metabase to create requests. (this will ignore username and password) | *optional* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `metabase::my-metabase/dashboard/5123` |
| `resource.name` | `dashboard_name` |
| `resource.service` | `metabase` |
| `description` | `table description` |
| `charts` | [][Chart](#chart) |

### Chart

| Field | Sample Value |
| :---- | :---- |
| `urn` | `metabase::my-metabase/card/9123` |
| `source` | `metabase` |
| `dashboard_urn` | `metabase::my-metabase/dashboard/5123` |
| `dashboard_source` | `metabase` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
