# couchdb

## Usage

```yaml
source:
  type: couchdb
  config:
    connection_url: http://admin:pass123@localhost:3306/
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `connection_url` | `string` | `http://admin:pass123@localhost:3306/` | URL to access the couchdb server | *required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `database_name.docID` |
| `resource.name` | `docID` |
| `resource.service` | `couchdb` |
| `schema` | [][Column](#column) |

### Column

| Field | Sample Value |
| :---- | :---- |
| `name` | `field1` |
| `description` | `rev for revision history` |
| `data_type` | `float64` |
| `length` | `` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
