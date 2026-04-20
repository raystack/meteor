# Compass

Send metadata to a Compass service via its Connect RPC (HTTP+JSON) endpoints.

## Usage

```yaml
sinks:
  - name: compass
    config:
      host: https://compass.com
      headers:
        Compass-User-UUID: meteor@raystack.io
        X-Other-Header: value1, value2
```

## Configuration

| Key | Type | Example | Description | |
| :-- | :--- | :------ | :---------- | :- |
| `host` | `string` | `https://compass.com` | Hostname of the Compass service | *required* |
| `headers` | `map` | `Compass-User-UUID: meteor@raystack.io` | Additional HTTP headers to send with each request. Multiple values are comma-separated. | *optional* |
| `max_concurrency` | `int` | `10` | Maximum concurrent requests per batch. `0` means no limit (default). | *optional* |

## Behavior

For each Record the sink:

1. **Upserts the Entity** via `POST /raystack.compass.v1beta1.CompassService/UpsertEntity` -- sends `urn`, `type`, `name`, `description`, `source`, and `properties`. Skipped for edge-only records (no name, description, or properties).
2. **Upserts all Edges** via `POST /raystack.compass.v1beta1.CompassService/UpsertEdge` -- one request per edge (e.g. `owned_by`, `derived_from`, `generates`), sending `source_urn`, `target_urn`, `type`, `source`, and `properties`.

Requests are made concurrently within a batch (controlled by `max_concurrency`). Server errors (5xx) are returned as retryable errors.

## Contributing

Refer to the [contribution guidelines](../../../docs/contribute/guide.mdx#adding-a-new-sink) for information on contributing to this module.
