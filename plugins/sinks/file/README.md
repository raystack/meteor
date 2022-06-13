# file

Sinks metadata to a file in `json/yaml` format as per the config defined.

## Usage

```yaml
sinks:
    name: file
    config:
        path: "./dir/sample.yaml"
        format: "yaml"
```

## Config Defination

| Key | Value | Example | Description |  |
| :-- | :---- | :------ | :---------- | :-- |
|`path` | `string` | `./dir/sample.yaml` | absolute or relative path from binary to output file, directory should exist| *required*|
| `format` | `string` | `yaml` | data format for the output file | *required* |

## Contributing

Refer to the contribution guidelines for information on contributing to this module.
