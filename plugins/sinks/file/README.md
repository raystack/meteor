# file

Sinks metadata to a file in `json/yaml` format as per the config defined.

## Usage

```yaml
sinks:
    name: file
    config:
        format: "yaml/json"
        filename: "postgres_server"
        output-dir: path/to/folder
```

## Config Defination

| Key | Value | Example | Description |  Default  | |
| :-- | :---- | :------ | :---------- | :-- | :-- |
|`format` | `string` | `yaml` | Define the format of file, currently supports yaml and json | `json` | *optional* |
|`filename` | `string` | `sample-name` | Define the filename for output | `NA` | *required*|
|`output` | `string` | `dataDir/d1/` | Path to output directory | `./` | *optional*|

## Contributing

Refer to the contribution guidelines for information on contributing to this module.
