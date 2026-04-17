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

## Behavior

For each Record the sink:

1. **Upserts the Entity** via `POST /raystack.compass.v1beta1.CompassService/UpsertEntity` -- sends `urn`, `type`, `name`, `description`, `source`, and `properties`. Lineage edges are included inline as `upstreams` and `downstreams` URN arrays.
2. **Upserts non-lineage Edges** via `POST /raystack.compass.v1beta1.CompassService/UpsertEdge` -- one request per edge (e.g. `owned_by`), sending `source_urn`, `target_urn`, `type`, `source`, and `properties`.

Requests are made concurrently within a batch. Server errors (5xx) are returned as retryable errors.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-sink) for information on contributing to this module.
