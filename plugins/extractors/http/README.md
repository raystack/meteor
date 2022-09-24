# HTTP

## Usage

```yaml
source:
  scope: odpf
  type: http
  config:
    request:
      url: "https://odpf.io"
      path: "employee/list"
      method: "GET"
      headers:
        "API-TOKEN": "XXXXX"
      content_type: application/json
      accept: application/json
    response:
      root: "data"
      mapping:
        urn: "work_email"
        name: "name.fullname"
        service: "workato"
        type: "user"
        data:
          email: "work_email"
          fullname: "name.fullname"
          status: "terminated"
          attributes:
            business_title: "CONST.employee"
            company_hierarchy: "company_hierarchy"
```

## Inputs

| Key | Value | Example | Description |    |
| :-- | :---- | :------ | :---------- | :- |
| `request.url` | `string` | `https://odpf.io` | HTTP service URL | *required* |
| `request.path` | `string` | `employee/list` | Path to the service | *not required* |
| `request.method` | `string` | `GET` | HTTP request type | *required* |
| `request.content_type` | `string` | `application/json` | Request body content type | *not required, default: application/json* |
| `request.accept` | `string` | `application/json` | Expected response's content type | *not required, default: application/json* |
| `request.headers` | `key-value` | `"API-TOKEN": "XXXXX"` | Headers in to form of key value pairs. Supports multiple values seperated by `,` | *not required* |
| `response.root` | `string` | `data` | `.` seperated [json path](https://jsonpath.com/) representation to the key containing data | *required* |
| `response.mapping` | `nested key-value` | `-` | Defines how the data is to be mapped with the asset model. Uses json path representation wrt `root` | *-* |
| `response.mapping.urn` | `string` | `urn: "work_email"` | A key that can be used as urn | *required* |
| `response.mapping.name` | `string` | `name: "fullname"` | Name of the asset | *required* |
| `response.mapping.service` | `string` | `service: "workato"` | Name of the HTTP service | *required* |
| `response.mapping.type` | `string` | `type: "user"` | One of `table, topic, dashboard, user, bucket, job` | *required* |
| `response.mapping.data` | `nested key-value` | `-` | Nested key-value based on the definition of asset type  | *required* |

### Note: 

To send constant values of fields in mapping, use `CONST` seperated by `.` and then the value.
For instance, `email: CONST.user@odpf.com`

## Outputs

Output can be any of the asset type.

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
