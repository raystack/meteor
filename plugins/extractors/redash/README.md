# redash

## Usage

```yaml
source:
  name: redash
  config:
    base_url: https://redash.example.com
    api_key: t33I8i8OFnVt3t9Bjj2RXr8nCBz0xyzVZ318Zwbj

```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `base_url` | `string` | `https://redash.example.com` | Each endpoint is appended to your Redash base URL | *required* |
| `api_key` | `string` | `t33I8i8OFnVt3t9Bjj2RXr8nCBz0xyzVZ318Zwbj` | Redash API calls support authentication with an API key | *required* |

## Outputs

| Field                  | Sample Value                                   |
|:-----------------------|:-----------------------------------------------|
| `resource.urn`         | `redash.dashboard_name`                        |
| `resource.name`        | `dashboard_slug`                               |
| `resource.service`     | `redash`                                       |
| `resource.type`        | `dashboard`                                    |
| `resource.url`         | `https://redash.example.com/dashboard_slug`    |
| `resource.description` | `ID: dashboard_id, version: dashboard_version` |

### Chart
Currently, no Redash public API is exposed to fetch charts detail in a particular dashboard.

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
