# mongodb

## Usage
```yaml
source:
  type: mongodb
  config:
    host: localhost:27017
    user_id: admin
    password: 1234
```
## Inputs
| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `host` | `string` | `localhost:27017` | The Host at which server is running | *required* |
| `user_id` | `string` | `admin` | User ID to access the mongo server| *required* |
| `password` | `string` | `1234` | Password for the Mongo Server | *required* |

## Outputs
| Field | Sample Value |
| :---- | :---- |
| `urn` | `my_database.my_collection` |
| `name` | `my_collection` |
| `source` | `mongodb` |
| `description` | `table description` |
| `profile.total_rows` | `2100` |

## Contributing
Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
