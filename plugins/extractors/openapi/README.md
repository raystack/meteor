# OpenAPI

Extract API schema metadata from OpenAPI (v2/v3) and gRPC (protobuf) definitions.

## Usage

```yaml
source:
  name: openapi
  scope: my-api
  config:
    source: ./specs/petstore.yaml
    format: openapi
    service: petstore
```

### Protobuf example

```yaml
source:
  name: openapi
  scope: my-service
  config:
    source: ./protos/*.proto
    format: protobuf
```

### Remote URL example

```yaml
source:
  name: openapi
  scope: my-api
  config:
    source: https://petstore3.swagger.io/api/v3/openapi.json
```

## Configuration

| Key | Type | Required | Description |
| :-- | :--- | :------- | :---------- |
| `source` | `string` | Yes | Path or URL to the spec file. Supports globs for local files (e.g. `./specs/*.yaml`). |
| `format` | `string` | No | Format of the spec: `openapi` or `protobuf`. Auto-detected from file extension if omitted. |
| `service` | `string` | No | Service name override. Defaults to `info.title` for OpenAPI or `package` name for protobuf. |

## Entities

### Entity: `api`

#### OpenAPI properties

| Field | Sample Value |
| :---- | :----------- |
| `urn` | `urn:openapi:my-api:api:petstore` |
| `name` | `Petstore` |
| `description` | `A sample API for pets` |
| `properties.version` | `1.0.0` |
| `properties.format` | `openapi_v3` |
| `properties.endpoints` | `[{"method": "GET", "path": "/pets", "summary": "List all pets", "operation_id": "listPets"}]` |
| `properties.schemas` | `5` |
| `properties.servers` | `["https://api.example.com/v1"]` |

#### Protobuf properties

| Field | Sample Value |
| :---- | :----------- |
| `urn` | `urn:openapi:my-service:api:mypackage` |
| `name` | `mypackage` |
| `properties.version` | `proto3` |
| `properties.format` | `protobuf` |
| `properties.package` | `mypackage` |
| `properties.services` | `[{"name": "UserService", "methods": [{"name": "GetUser", "input_type": "GetUserRequest", "output_type": "GetUserResponse"}]}]` |
| `properties.messages` | `10` |

### Edges

No edges are emitted by this extractor.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-extractor) for information on contributing to this module.
