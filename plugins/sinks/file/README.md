# file

Sinks metadata to a file in `ndjson/yaml` format as per the config defined.

## Usage

```yaml
sinks:
    name: file
    config:
        path: "./dir/sample.yaml"
        format: "yaml"
        overwrite: false
```

## Config Defination

| Key | Value | Example | Description |  |
| :-- | :---- | :------ | :---------- | :-- |
|`path` | `string` | `./dir/sample.yaml` | absolute or relative path from binary to output file, directory should exist| *required*|
| `format` | `string` | `yaml` | data format for the output file | *required* |
| `overwrite` | `bool` | `false` | to choose whether data should be overwritten or appended in case file exists, default is `true` | *optional* |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-sink) for information on contributing to this module.
