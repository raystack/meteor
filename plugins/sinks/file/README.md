# file

Sinks metadata to a file in `json/yaml` format as per the config defined.

## Usage

```yaml
sinks:
    name: file
    config:
        path: "./dir/sample.yaml"
```

## Config Defination

| Key | Value | Example | Description |  |
| :-- | :---- | :------ | :---------- | :-- |
|`path` | `string` | `./dir/sample.yaml` | path to output file, directory should exist, file should be either yaml or json  | *required*|

## Contributing

Refer to the contribution guidelines for information on contributing to this module.
