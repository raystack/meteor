# Http

Compass is a search and discovery engine built for querying application deployments, datasets and meta resources. It can also optionally track data flow relationships between these resources and allow the user to view a representation of the data flow graph.

## Usage

```yaml
sinks:
  name: http
  config:
    method: POST
    success_code: 200
    url: https://compass.requestcatcher.com/v1beta2/asset/{{ .Type }}/{{ .Urn }}
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

## Config Defination

| Key | Value | Example | Description |  |
| :-- | :---- | :------ | :---------- | :-- |
|`url` | `string` | `https://compass.requestcatcher.com/v1beta2/asset/{{ .Type }}/{{ .Urn }}` | URL to the http server, contains all the info needed to make the request, like port and route, support go [text/template](https://pkg.go.dev/text/template) (see the properties in [v1beta2.Asset](https://github.com/goto/meteor/blob/main/models/gotocompany/assets/v1beta2/asset.pb.go#L25-L68))  | *required*|
| `method` | `string` | `POST` | the method string of by which the request is to be made, e.g. POST/PATCH/GET | *required* |
| `success_code` | `integer` | `200` |  to identify the expected success code the http server returns, defult is `200` | *optional* |
| `headers` | `map` | `"Content-Type": "application/json"` | to add any header/headers that may be required for making the request | *optional* |
| `script` | `Object` | see [Script](#Script)   | Script for building custom payload | *optional |

## Script

| Key | Value | Example | Description |  |
| :-- | :---- | :------ | :---------- | :-- |
| `engine` | `string` | `tengo`                                | Script engine. Only `"tengo"` is supported currently                                            |  *required* |
| `source` | `string` | see [Usage](#Usage). | [Tengo][tengo] script used to map the request into custom payload to be sent to url.                           |  *required* |

### Script Globals

- [`asset`](#recipe_scope)
- [`sink(Payload)`](#sinkpayload)
- [`exit`](#exit)

#### `asset`

The asset record emitted by the extractor and processors is made available in the script
environment as `asset`. The field names will be as
per the [`Asset` proto definition](https://github.com/goto/proton/blob/5b5dc72/gotocompany/assets/v1beta2/asset.proto#L14). 

#### `sink(Payload)`

Send http request to url with payload.


#### `exit()`

Terminates the script execution.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-sink) for information on contributing to this module.
