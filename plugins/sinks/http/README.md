# Http

Send metadata to any HTTP endpoint. Can optionally transform the payload using a Tengo script before sending.

## Usage

```yaml
sinks:
  name: http
  config:
    method: POST
    success_code: 200
    url: https://compass.requestcatcher.com/v1/entity/{{ .Type }}/{{ .Urn }}
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

## Config Definition

| Key            | Value     | Example                                                                    | Description                                                                                                                                                                                               |            |
| :------------- | :-------- | :------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------- |
| `url`          | `string`  | `https://compass.requestcatcher.com/v1/entity/{{ .Type }}/{{ .Urn }}`      | URL to the http server. Supports Go [text/template](https://pkg.go.dev/text/template) with Entity fields (`Urn`, `Type`, `Name`, `Source`, `Description`).                                               | _required_ |
| `method`       | `string`  | `POST`                                                                     | The HTTP verb/method for the request, e.g. POST/PATCH/GET                                                                                                                                                 | _required_ |
| `success_code` | `integer` | `200`                                                                      | Expected success status code. Default is `200`.                                                                                                                                                           | _optional_ |
| `headers`      | `map`     | `"Content-Type": "application/json"`                                       | Headers to send with the request.                                                                                                                                                                         | _optional_ |
| `script`       | `Object`  | see [Script](#Script)                                                      | Script for building custom payload.                                                                                                                                                                       | _optional_ |

## Script

| Key      | Value    | Example              | Description                                                                          |            |
| :------- | :------- | :------------------- | :----------------------------------------------------------------------------------- | :--------- |
| `engine` | `string` | `tengo`              | Script engine. Only `"tengo"` is supported currently                                 | _required_ |
| `source` | `string` | see [Usage](#Usage). | [Tengo][tengo] script used to map the entity into a custom payload to be sent to url. | _required_ |

### Script Globals

- [`asset`](#asset)
- [`sink(Payload)`](#sinkpayload)
- [`exit`](#exit)

#### `asset`

The entity record emitted by the extractor and processors is made available in
the script environment as `asset`. Note: the variable is still called `asset`
in tengo scripts for backward compatibility, but it represents an Entity.

The `asset` object has the following fields: `urn`, `type`, `name`,
`description`, `source`, and `properties` (flat key-value map with all
type-specific metadata).

#### `sink(Payload)`

Send http request to url with payload.

#### `exit()`

Terminates the script execution.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-sink) for information on contributing to this module.

[tengo]: https://github.com/d5/tengo
