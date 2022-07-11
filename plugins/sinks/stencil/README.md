# Stencil

Stencil is a schema registry that provides schema management and validation dynamically, efficiently, and reliably to ensure data compatibility across applications.

## Usage

```yaml
sinks:
  name: stencil
  config:
    host: https://stencil.com
    namespace_id: test-namespace
    schema_id: example
    format: json
    send_format_header: false
```

## Config Definition

| Key | Value | Example | Description |  |
| :-- | :---- | :------ | :---------- | :-- |
|`host` | `string` | `https://stencil.com` | The hostname of the stencil service | *required*|
| `namespace_id` | `string` | `myNamespace` | The namespace ID of the stencil service | *required* |
|`schema_id` | `string` | `mySchmea` | The schema ID which will be created in the above-mentioned namespace | *required*|
|`format` | `string` | `json` | The schema format in which data will sink to stencil | *optional*|
|`send_format_header` | `bool` | `false` | If schema format needs to be changed. Suppose changing format from json to avro,
provide below config value as true and schema format in format config. | *optional*|


## Contributing

Refer to the contribution guidelines for information on contributing to this module.