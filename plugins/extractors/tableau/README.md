# tableau

## Usage
### Note
To use tableau extractor, you need to enable [metadata service api](https://help.tableau.com/current/api/metadata_api/en-us/)
```yaml
source:
  type: tableau
  config:
    host: http://server.tableau.com
    version: 3.12
    username: meteor_user
    password: xxxxxxxxxx
```



## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `host` | `string` | `https://server.tableau.com`         | The host at which tableau is running | *required* |
| `version` | `string` | `3.12`     | The version of [Tableau REST API](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_versions.htm), tested with 3.12 | *required* |
| `username` | `string` | `meteor_user` | Username/email to access the tableau | *required* |
| `password` | `string` | `xxxxxxxxxx` | Password for the tableau | *required* |
| `sitename` | `string` | `testdev550928` | The name of your tableau site, it will point to the default one if you leave it empty | *not required* |

## Outputs

| Field | Sample Value |
| :---- | :---- |
| `resource.urn` | `tableau::{host}/workbook/{workbook_id}` |
| `resource.name` | `workbook_name` |
| `resource.service` | `tableau` |
| `resource.description` | `a description of the dashboard` |
| `charts` | [][Chart](#chart) |

### Chart

| Field | Sample Value |
| :---- | :---- |
| `urn` | `tableau::{host}/sheet/{sheet_id}`             |
| `source` | `tableau` |
| `dashboard_urn` | `tableau::{host}/workbook/{workbook_id}` |
| `dashboard_source` | `tableau` |

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
