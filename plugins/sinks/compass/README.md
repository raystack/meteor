# Compass

Compass is a search and discovery engine built for querying application deployments, datasets and meta resources. It can also optionally track data flow relationships between these resources and allow the user to view a representation of the data flow graph.

This sink targets Compass v2's entity-graph API using Connect RPC (HTTP+JSON) endpoints.

## Usage

```yaml
sinks:
  name: compass
  config:
    host: https://compass.com
    headers:
      Compass-User-UUID: meteor@raystack.io
      X-Other-Header: value1, value2
```

## How It Works

For each record, the sink:

1. **Upserts an entity** via `POST /raystack.compass.v1beta1.CompassService/UpsertEntity` — maps asset URN, type, name, description, service (as source), and flattens data/labels/URL into a single `properties` field.
2. **Sends lineage** inline with the entity request as `upstreams` and `downstreams` URN arrays.
3. **Upserts ownership edges** via `POST /raystack.compass.v1beta1.CompassService/UpsertEdge` — one `owned_by` edge per owner.

## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-sink) for information on contributing to this module.
