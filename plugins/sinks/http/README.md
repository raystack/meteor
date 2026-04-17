# HTTP

Send metadata to any HTTP endpoint, with optional Tengo script transforms.

## Usage

```yaml
sinks:
  - name: http
    config:
      method: POST
      success_code: 200
      url: "https://example.com/v1/entity/{{ .Type }}/{{ .Urn }}"
      headers:
        Header-1: value11,value12
      script:
        engine: tengo
        source: |
          payload := {
            details: {
              some_key: asset.urn,
              another_key: asset.name
            }
          }
          sink(payload)
```

## Configuration

| Key | Type | Example | Description | |
| :-- | :--- | :------ | :---------- | :- |
| `url` | `string` | `https://example.com/v1/entity/{{ .Type }}/{{ .Urn }}` | URL of the HTTP endpoint. Supports Go [text/template](https://pkg.go.dev/text/template) with Entity fields: `Urn`, `Type`, `Name`, `Source`, `Description`. | *required* |
| `method` | `string` | `POST` | HTTP method (e.g. `POST`, `PUT`, `PATCH`) | *required* |
| `success_code` | `int` | `200` | Expected HTTP status code for success. Default: `200`. | *optional* |
| `headers` | `map` | `Content-Type: application/json` | Additional HTTP headers. Multiple values are comma-separated. | *optional* |
| `script` | `object` | see below | Script for building a custom payload. | *optional* |

### Script

| Key | Type | Example | Description | |
| :-- | :--- | :------ | :---------- | :- |
| `engine` | `string` | `tengo` | Script engine. Only `tengo` is supported. | *required* |
| `source` | `string` | see [Usage](#usage) | [Tengo](https://github.com/d5/tengo) script that builds the request payload. | *required* |

**Script globals:**

- `asset` -- The Entity object with fields: `urn`, `type`, `name`, `description`, `source`, `properties`, `create_time`, `update_time`. (Named `asset` for backward compatibility.)
- `sink(payload)` -- Sends the HTTP request with the given payload.
- `exit()` -- Terminates script execution.

## Behavior

For each Record, the Entity is sent as a JSON payload to the configured URL. If a `script` is provided, the script builds a custom payload instead of using the default Entity JSON. Server errors (5xx) are returned as retryable errors.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-sink) for information on contributing to this module.
