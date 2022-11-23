# http

Generic Extractor capable of using the HTTP response from an external API for
constructing the following assets types:

- [`Bucket`][proton-bucket]
- [`Dashboard`][proton-dashboard]
- [`Experiment`][proton-experiment]
- [`FeatureTable`][proton-featuretable]
- [`Group`][proton-group]
- [`Job`][proton-job]
- [`Metric`][proton-metric]
- [`Model`][proton-model]
- [`Application`][proton-application]
- [`Table`][proton-table]
- [`Topic`][proton-topic]
- [`User`][proton-user]

The user specified script has access to the response, if the API call was
successful, and can use it for constructing and emitting assets using a custom
script. Currently, [Tengo][tengo] is the only supported script engine.

Refer Tengo documentation for script language syntax and supported functionality
\- https://github.com/d5/tengo/tree/v2.13.0#references
. [Tengo standard library modules][tengo-stdlib] can also be imported and used
if required.

## Usage

```yaml
source:
  scope: odpf
  type: http
  config:
    request:
      url: "https://example.com/api/v1/endpoint"
      query_params:
        - key: param_key
          value: param_value
      method: "POST"
      headers:
        "User-Id": "1a4336bc-bc6a-4972-83c1-d6426b4d79c3"
      content_type: application/json
      accept: application/json
      body:
        key: value
      timeout: 5s
    success_codes: [ 200 ]
    script:
      engine: tengo
      source: |
        asset := new_asset("user")
        // modify the asset using 'response'...
        emit(asset)
```

## Inputs

| Key                    | Value               | Example                                | Description                                                                                     | Required? |
|:-----------------------|:--------------------|:---------------------------------------|:------------------------------------------------------------------------------------------------|:----------|
| `request.url`          | `string`            | `http://example.com/api/v1/endpoint`   | The HTTP endpoint to send request to                                                            | ✅         |
| `request.query_params` | `[]{key, value}`    | `[{"key":"s","value":"One Piece"}]`    | The query parameters to be added to the request URL.                                            | ✘         |
| `request.method`       | `string`            | `GET`/`POST`                           | The HTTP verb/method to use with request. Default is `GET`.                                     | ✘         |
| `request.headers`      | `map[string]string` | `{"Api-Token": "..."}`                 | Headers to send in the HTTP request.                                                            | ✘         |
| `request.content_type` | `string`            | `application/json`                     | Content type for encoding request body. Also sent as a header.                                  | ✅         |
| `request.accept`       | `string`            | `application/json`                     | Sent as the `Accept` header. Also indicates the format to use for decoding.                     | ✅         |
| `request.body`         | `Object`            | `{"key": "value"}`                     | The request body to be sent.                                                                    | ✘         |
| `request.timeout`      | `string`            | `1s`                                   | Timeout for the HTTP request. Default is 5s.                                                    | ✘         |
| `success_codes`        | `[]int`             | `[200]`                                | The list of status codes that would be considered as a successful response. Default is `[200]`. | ✘         |
| `script.engine`        | `string`            | `tengo`                                | Script engine. Only `"tengo"` is supported currently                                            | ✅         |
| `script.source`        | `string`            | see [Worked Example](#worked-example). | [Tengo][tengo] script used to map the response into 0 or more assets.                           | ✅         |

### Notes

- In case of conflicts between query parameters present in `request.url`
  and `request.query_params`, `request.query_params` takes precedence.
- Currently, only `application/json` is supported for encoding the request body
  and for decoding the response body. If `Content-Type` and `Accept` headers are
  added under `request.headers`, they will be ignored and overridden.
- Script is only executed if the response status code matches
  the `success_codes` provided.
- Tengo is the only supported script engine.
- Tengo's `os` stdlib module cannot be imported and used in the script.

### Script Globals

#### `response`

HTTP response received.

#### `new_asset(string): Asset`

Takes a single string parameter and returns an asset instance. The `type`
parameter can be one of the following:

- `"bucket"` ([proto][proton-bucket])
- `"dashboard"` ([proto][proton-dashboard])
- `"experiment"` ([proto][proton-experiment])
- `"feature_table"` ([proto][proton-featuretable])
- `"group"` ([proto][proton-group])
- `"job"` ([proto][proton-job])
- `"metric"` ([proto][proton-metric])
- `"model"` ([proto][proton-model])
- `"application"` ([proto][proton-application])
- `"table"` ([proto][proton-table])
- `"topic"` ([proto][proton-topic])
- `"user"` ([proto][proton-user])

The asset can then be modified in the script to set properties that are
available for the given asset type.

**WARNING:** Do not overwrite the `data` property, set fields on it instead.
Translating script object into proto fails otherwise.

```go
// Bad
asset.data = {full_name: "Daiyamondo Jozu"}

// Good
asset.data.full_name = "Daiyamondo Jozu"
```

#### `emit(Asset)`

Takes an asset and emits the asset that can then be consumed by the
processor/sink.

#### `exit()`

Terminates the script execution.

## Output

The output of the extractor depends on the user specified script. It can emit 0
or more assets.

### Worked Example

Lets consider a service that returns a list of users on making a `GET` call on
the endpoint `http://my_user_service.company.com/api/v1/users` in the following
format:

```json
{
  "success": "<bool>"
  "message": "<string>",
  "data": [
    {
      "manager_name": "<string>",
      "terminated": "<string: true/false>",
      "fullname": "<string>",
      "location_name": "<string>",
      "work_email": "<string: email>",
      "supervisory_org_id": "<string>",
      "supervisory_org_name": "<string>",
      "preferred_last_name": "<string>",
      "business_title": "<string>",
      "company_name": "<string>",
      "cost_center_id": "<string>",
      "preferred_first_name": "<string>",
      "product_name": "<string>",
      "cost_center_name": "<string>",
      "employee_id": "<string>",
      "manager_id": "<string>",
      "location_id": "<string: ID/IN>",
      "manager_id_2": "<string>",
      "termination_date": "<string: YYYY-MM-DD>",
      "company_hierarchy": "<string>",
      "company_id": "<string>",
      "preferred_middle_name": "<string>",
      "preferred_social_suffix": "<string>",
      "legal_middle_name": "<string>",
      "manager_email_2": "<string: email>",
      "legal_first_name": "<string>",
      "manager_name_2": "<string>",
      "manager_email": "<string: email>",
      "legal_last_name": "<string>"
    }
  ]
}
```

Assuming the authentication can be done using an `Api-Token` header, we can use
the following recipe:

```yaml
source:
  scope: production
  type: http
  config:
    request:
      url: "http://my_user_service.company.com/api/v1/users"
      method: "GET"
      headers:
        "Api-Token": "1a4336bc-bc6a-4972-83c1-d6426b4d79c3"
      content_type: application/json
      accept: application/json
      timeout: 5s
    success_codes: [ 200 ]
    script:
      engine: tengo
      source: |
        if !response.success {
          exit()
        }

        users := response.data
        for u in users {
          if u.email == "" {
            continue
          }

          asset := new_asset("user")
          // URN format: "urn:{service}:{scope}:{type}:{id}"
          asset.urn = format("urn:%s:staging:user:%s", "my_usr_svc", u.employee_id)
          asset.name = u.fullname
          asset.service = "my_usr_svc"
          // asset.type = "user" // not required, new_asset("user") sets the field.
          asset.data.email = u.work_email
          asset.data.username = u.employee_id
          asset.data.first_name = u.legal_first_name
          asset.data.last_name = u.legal_last_name
          asset.data.full_name = u.fullname
          asset.data.display_name = u.fullname
          asset.data.title = u.business_title
          asset.data.status = u.terminated == "true" ? "suspended" : "active"
          asset.data.manager_email = u.manager_email
          asset.data.attributes = {
            manager_id:           u.manager_id,
            cost_center_id:       u.cost_center_id, 
            supervisory_org_name: u.supervisory_org_name,
            location_id:          u.location_id
          }
          emit(asset)
        }
```

This would emit a 'User' asset for each user object in `response.data`.

## Caveats

The following features are currently not supported:

- Pagination.
- Explicit authentication support, ex: Basic auth/OAuth/OAuth2/JWT etc.
- Retries with configurable backoff.
- Content type for request/response body other than `application/json`.

## Contributing

Refer to
the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor)
for information on contributing to this module.

[proton-bucket]: https://github.com/odpf/proton/blob/fabbde8/odpf/assets/v1beta2/bucket.proto#L13

[proton-dashboard]: https://github.com/odpf/proton/blob/fabbde8/odpf/assets/v1beta2/dashboard.proto#L14

[proton-experiment]: https://github.com/odpf/proton/blob/fabbde8/odpf/assets/v1beta2/experiment.proto#L15

[proton-featuretable]: https://github.com/odpf/proton/blob/fabbde8/odpf/assets/v1beta2/feature_table.proto#L32

[proton-group]: https://github.com/odpf/proton/blob/fabbde8/odpf/assets/v1beta2/group.proto#L12

[proton-job]: https://github.com/odpf/proton/blob/fabbde8/odpf/assets/v1beta2/job.proto#L13

[proton-metric]: https://github.com/odpf/proton/blob/fabbde8/odpf/assets/v1beta2/metric.proto#L13

[proton-model]: https://github.com/odpf/proton/blob/fabbde8/odpf/assets/v1beta2/model.proto#L17

[proton-application]: https://github.com/odpf/proton/blob/fabbde8/odpf/assets/v1beta2/application.proto#L11

[proton-table]: https://github.com/odpf/proton/blob/fabbde8/odpf/assets/v1beta2/table.proto#L14

[proton-topic]: https://github.com/odpf/proton/blob/fabbde8/odpf/assets/v1beta2/topic.proto#L14

[proton-user]: https://github.com/odpf/proton/blob/fabbde8/odpf/assets/v1beta2/user.proto#L15

[tengo]: https://github.com/d5/tengo

[tengo-stdlib]: https://github.com/d5/tengo/blob/v2.13.0/docs/stdlib.md
