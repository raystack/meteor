# http

Extract metadata from any external HTTP API using a user-defined [Tengo](https://github.com/d5/tengo) script.

## Usage

```yaml
source:
  scope: production
  type: http
  config:
    request:
      route_pattern: "/api/v1/users"
      url: "https://example.com/api/v1/users"
      method: "GET"
      headers:
        "Api-Token": "my-secret-token"
      content_type: application/json
      accept: application/json
      timeout: 5s
    success_codes: [200]
    concurrency: 5
    script:
      engine: tengo
      source: |
        for u in response.body.users {
          asset := new_asset("user")
          asset.urn = format("urn:my_svc:%s:user:%s", recipe_scope, u.id)
          asset.name = u.full_name
          asset.source = "my_svc"
          asset.properties.email = u.email
          asset.properties.status = u.active ? "active" : "suspended"
          emit(asset)
        }
```

## Configuration

| Key | Type | Required | Default | Description |
|:----|:-----|:---------|:--------|:------------|
| `request.route_pattern` | `string` | Yes | | Route pattern used as `http.route` metric tag. |
| `request.url` | `string` | Yes | | HTTP endpoint URL. |
| `request.method` | `string` | No | `GET` | HTTP method (`GET` or `POST`). |
| `request.headers` | `map[string]string` | No | | HTTP request headers. |
| `request.content_type` | `string` | Yes | | Content type for the request body (only `application/json`). |
| `request.accept` | `string` | Yes | | Accept header / response decode format (only `application/json`). |
| `request.body` | `object` | No | | Request body. |
| `request.query_params` | `[]{key, value}` | No | | Query parameters appended to the URL. |
| `request.timeout` | `string` | No | `5s` | Request timeout. |
| `success_codes` | `[]int` | No | `[200]` | HTTP status codes considered successful. |
| `concurrency` | `int` | No | `5` | Concurrency for child requests via `execute_request`. |
| `script.engine` | `string` | Yes | | Script engine (only `tengo`). |
| `script.source` | `string` | Yes | | Tengo script source code. |
| `script.max_allocs` | `int` | No | `5000` | Max object allocations during script execution. |
| `script.max_const_objects` | `int` | No | `500` | Max constant objects in compiled script. |
| `before_script.engine` | `string` | No | | Script engine for a pre-request script. |
| `before_script.source` | `string` | No | | Tengo script executed before the main request. |

### Notes

- Only `application/json` is supported for request/response encoding.
- Query params in `request.query_params` take precedence over those in `request.url`.
- The script runs only if the response status code matches `success_codes`.
- The Tengo `os` stdlib module is not available.

## Script interface

The following globals are available in the Tengo script:

### `recipe_scope`

The `scope` value from the recipe (string).

### `response`

HTTP response object with `status_code`, `header`, and `body`. Header names are lower-cased.

```json
{
  "status_code": "200",
  "header": { "content-type": "application/json" },
  "body": { "users": [...] }
}
```

### `new_asset(type) -> entity`

Creates a new entity map of the given type (e.g. `"user"`, `"table"`, `"topic"`, `"job"`, `"dashboard"`, `"bucket"`, `"model"`, `"application"`, etc.).

Set fields on the returned map:
- `asset.urn` - Entity URN (string)
- `asset.name` - Entity name (string)
- `asset.source` - Source system name (string)
- `asset.description` - Description (string)
- `asset.properties.*` - Type-specific metadata as flat key-value pairs

**Important:** Do not overwrite `asset.properties` as a whole; set individual keys instead.

### `emit(entity)`

Emits the entity as a Record to the pipeline.

### `execute_request(...requests) -> []response`

Executes one or more HTTP requests concurrently (up to `concurrency`). Each request object supports the same fields as `request` in the configuration. Returns an array where each item is either a response or an error.

```go
reqs := []
for item in response.body.items {
  reqs = append(reqs, {
    url: format("https://api.example.com/items/%s", item.id),
    method: "GET",
    content_type: "application/json",
    accept: "application/json",
    timeout: "5s"
  })
}
results := execute_request(reqs...)
for r in results {
  if is_error(r) { continue }
  asset := new_asset("job")
  asset.urn = format("urn:my_svc:%s:job:%s", recipe_scope, r.body.id)
  asset.name = r.body.name
  asset.source = "my_svc"
  emit(asset)
}
```

### `exit()`

Terminates script execution.

## Entities

The output depends entirely on the user-defined script. The script can emit zero or more entities of any supported type via `new_asset` and `emit`.

## Edges

This extractor does not emit edges directly. Edges can be constructed within the script if needed.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
