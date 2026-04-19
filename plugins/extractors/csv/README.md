# CSV

Extract column metadata from CSV files.

## Usage

```yaml
source:
  name: csv
  config:
    path: ./path-to-a-file-or-a-directory
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `path` | `string` | Yes | Path to a `.csv` file or a directory. When a directory is given, all `.csv` files directly inside it are extracted. |

## Entities

- Entity type: `table`
- URN format: `urn:csv:{scope}:file:{filename.csv}`

| Property | Type | Description |
| :------- | :--- | :---------- |
| `properties.columns` | `[]object` | List of column objects derived from the CSV header row. |
| `properties.columns[].name` | `string` | Column header name. |

## Edges

This extractor does not emit edges.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
