# csv

## Usage

```yaml
source:
  name: csv
  config:
    path: ./path-to-a-file-or-a-directory
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `path` | `string` | `./folder/filename.csv` | Path to a file or a directory | *required* |

### *Notes*

Passing directory in `path` will collect and extract metadata from all `.csv` files directly inside the directory path.

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `filename.csv` |
| `resource.name` | `filename.csv` |
| `resource.service` | `csv` |
| `schema.columns` | [][Column](#column) |

### Column

| Field | Sample Value | Description |
| :---- | :----------- | :---------- |
| `name` | `order_id` | csv header e.g. first row |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
