# couchdb

## Usage

```yaml
source:
  name: couchdb
  config:
    connection_url: http://admin:pass123@localhost:3306/
    exclude: database_a,database_b
```

## Inputs

| Key              | Value    | Example                                | Description                                                |            |
| :--------------- | :------- | :------------------------------------- | :--------------------------------------------------------- | :--------- |
| `connection_url` | `string` | `http://admin:pass123@localhost:3306/` | URL to access the couchdb server                           | _required_ |
| `exclude`        | `string` | `primaryDB,secondaryDB`                | Comma separated database list to be excluded from crawling | _optional_ |

## Outputs

| Field              | Sample Value          |
| :----------------- | :-------------------- |
| `resource.urn`     | `database_name.docID` |
| `resource.name`    | `docID`               |
| `resource.service` | `couchdb`             |
| `schema`           | [][column](#column)   |

### Column

| Field         | Sample Value               |
| :------------ | :------------------------- |
| `name`        | `field1`                   |
| `description` | `rev for revision history` |
| `data_type`   | `float64`                  |
| `length`      | ``                         |

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
