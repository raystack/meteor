# Meteor

Meteor is a plugin-driven metadata collection agent. It extracts metadata from data stores/services via **extractors**, transforms it via **processors**, and pushes it to catalog services via **sinks**.

## Architecture

```
Recipe (YAML) → Extractor → Processor(s) → Sink(s)
```

Each extractor emits **Records**. A Record contains:
- **Entity**: urn, type, name, description, source, properties (flat structpb.Struct)
- **Edges**: list of relationships, each with source_urn, target_urn, type, source, properties

Ownership is represented as edges with type `owned_by`. Lineage (upstreams/downstreams) is represented as edges with type `lineage`.

- **Extractors**: 34+ plugins (bigquery, postgres, kafka, github, etc.)
- **Processors**: Transform/enrich records in-flight
- **Sinks**: Push to destinations (compass, kafka, file, http, etc.)
- **Agent**: Orchestrates the pipeline with batching, retries, concurrency

## Key Directories

```
models/          Core data model (Record wrapping Entity + Edges)
plugins/
  extractors/    Source plugins (one dir per source)
  processors/    Transform plugins
  sinks/         Destination plugins (compass, kafka, file, etc.)
agent/           Pipeline orchestration
recipe/          Recipe parsing and validation
cmd/             CLI commands (run, lint, list, info, gen)
```

## Data Model

**Entity** (`meteorv1beta1.Entity`):
- `urn` - Unique resource name
- `type` - Entity type (table, dashboard, topic, job, user, repository, team, bucket, application, model, etc.)
- `name` - Human-readable name
- `description` - Description
- `source` - Source system (e.g. bigquery, postgres, kafka)
- `properties` - Flat key-value map (structpb.Struct) holding all type-specific metadata

**Edge** (`meteorv1beta1.Edge`):
- `source_urn` - URN of the source entity
- `target_urn` - URN of the target entity
- `type` - Relationship type (`owned_by`, `lineage`, `member_of`, etc.)
- `source` - Source system
- `properties` - Additional metadata

## Compass Integration

The Compass sink (`plugins/sinks/compass/`) sends entities and edges to Compass. Each Record is an Entity with flat properties, plus Edges for ownership and lineage.

## Build & Test

```
go build ./...
go test ./...
make lint
```

## Plan: Align Meteor with Compass v2

See `.claude/plans/compass-v2-alignment.md` for the implementation plan.
