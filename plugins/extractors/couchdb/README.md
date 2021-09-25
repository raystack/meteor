# couchdb

## Usage

```yaml
source:
  type: couchdb
  config:
    host: localhost:5984
    user_id: admin
    password: couchdb
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `host` | `string` | `localhost:5984` | The Host at which server is running | *required* |
| `user_id` | `string` | `admin` | User ID to access the couchdb server| *required* |
| `password` | `string` | `couchdb` | Password for the couchdb Server | *required* |

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
