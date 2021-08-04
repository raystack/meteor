# metabase

## Usage

```yaml
source:
  type: metabase
  config:
    host: http://localhost:3000
    user_id: meteor_tester
    password: meteor_pass_1234
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `host` | `string` | `http://localhost:3000` | The url at which metabase is running | *required* |
| `user_id` | `string` | `meteor_tester` | User ID to access the metabase| *required* |
| `password` | `string` | `meteor_pass_1234` | Password for the metabase | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `urn` | `metabase.dashboard_name` |
| `name` | `dashboard_name` |
| `source` | `metabase` |
| `description` | `table description` |
| `schema` | [][Chart](#chart) |

### Chart
| Field | Sample Value |
| :---- | :---- |


## Contributing
Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
