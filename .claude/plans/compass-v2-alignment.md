# Plan: Native Entity Model in Meteor

## Context

Meteor currently wraps everything in `v1beta2.Asset` — a rigid proto with typed `Data` (anypb.Any wrapping Table/Dashboard/Topic/etc.), separate `Owners`, `Lineage`, `Labels`, and `URL` fields. The compass sink converts this to Compass v2's entity+edge model at the boundary.

This plan replaces Asset with Entity+Edge as Meteor's **native** data model. No phased migration — one clean cut.

### Why

1. **Asset is a Compass v1 artifact.** The typed Any wrapper (Table, Dashboard, Topic) serves no purpose outside Compass v1's PATCH endpoint. Compass v2, and every other graph-based catalog, wants entities with flat properties.
2. **Relationships are first-class in Compass v2.** Lineage, ownership, has_column — all are edges. Stuffing them into Asset fields (Lineage struct, Owners array) loses this uniformity.
3. **Open type system.** Compass v2 doesn't gatekeep types. Meteor shouldn't either. A typed Any proto forces a proto definition for every new asset kind.
4. **The sink shouldn't know domain logic.** Conversion from Asset→Entity is domain logic that belongs at the extraction point, not the delivery point.

## What Exists Today

### Proton (proto definitions)

```
raystack/proton/raystack/assets/v1beta2/     ← Meteor's current model (14 protos)
  asset.proto          Asset wrapper (URN, name, service, type, data Any, owners, lineage, labels)
  common.proto         Event, Lineage, Resource, Owner
  table.proto          Table, Column, ColumnProfile, TableProfile
  dashboard.proto      Dashboard, Chart
  topic.proto          Topic, TopicProfile, TopicSchema
  job.proto            Job
  user.proto           User, Profile, Membership
  group.proto          Group, Member
  bucket.proto         Bucket, Blob
  application.proto    Application
  model.proto          Model, ModelVersion, ModelSignature
  metric.proto         Metric
  feature_table.proto  FeatureTable, Feature
  experiment.proto     Experiment, Variant

raystack/proton/raystack/compass/v1beta1/     ← Compass v2's API (already defined)
  service.proto        Entity, Edge, UpsertEntity, UpsertEdge, etc.
```

### Meteor model layer

```go
// models/record.go
type Record struct {
    data *v1beta2.Asset
}
```

### Blast radius

| Layer | Files | What they do with Asset |
|-------|-------|------------------------|
| Record | `models/record.go`, `models/util.go` | Wrap and serialize Asset |
| Plugin interfaces | `plugins/plugin.go` | Emit, Process, Sink signatures |
| Extractors (31) | `plugins/extractors/*/` | Build Asset + anypb.New(TypedData) |
| Processors (3) | enrich, labels, script | Modify Asset fields |
| Sinks (8) | compass, kafka, file, http, console, stencil, frontier, gcs | Read Asset fields, serialize |
| Agent | `agent/agent.go`, `stream.go`, `batch.go` | Pass Records through pipeline |
| Utilities | `utils/custom_properties.go`, `structmap/` | Asset field access helpers |

### Extractor breakdown

| Asset Type | Extractors | Proto Type | Lineage | Owners |
|-----------|------------|------------|---------|--------|
| table | bigquery, bigtable, cassandra, clickhouse, couchdb, csv, elastic, mariadb, mongodb, mssql, mysql, oracle, postgres, presto, redshift, snowflake | Table | bigquery only | — |
| dashboard | grafana, metabase, redash, superset, tableau | Dashboard | metabase, tableau | tableau |
| topic | kafka | Topic | — | — |
| job | optimus | Job | ✓ | ✓ |
| user | frontier, github, gsuite | User | — | — |
| bucket | gcs | Bucket | — | — |
| model | merlin | Model | ✓ | ✓ |
| feature_table | caramlstore | FeatureTable | ✓ | — |
| application | application_yaml | Application | ✓ | ✓ |
| varies | http | varies | — | — |

## Design Decisions

### 1. Meteor defines its own protos, decoupled from Compass

**Decision: Define Meteor's model proto in `raystack/proton/raystack/meteor/v1beta1/`.**

Meteor gets its own Entity and Edge definitions. The Compass sink maps Meteor's model to Compass's API — a thin translation layer.

**Why decoupled:**

- **Independent evolution.** Compass can change its API (add fields, rename, add validation) without breaking Meteor's internal pipeline. Meteor's model is its own contract.
- **Clean separation of concerns.** Meteor is a producer. Compass is a consumer. They share a concept but don't need to share a proto. The sink is the translation boundary.
- **Future convergence possible.** If the protos stay identical over time, we can collapse them later. Starting coupled is harder to undo than starting decoupled.

### 2. What does the proto look like?

```protobuf
syntax = "proto3";
package raystack.meteor.v1beta1;

import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";

option go_package = "github.com/raystack/proton/meteor/v1beta1;meteorv1beta1";

// Entity is a named resource in the metadata graph.
message Entity {
  string urn = 1;
  string type = 2;              // open: "table", "dashboard", "topic", "satellite", anything
  string name = 3;
  string description = 4;
  string source = 5;            // who reported this: "bigquery", "postgres", "kafka", ...
  google.protobuf.Struct properties = 6;  // all type-specific data lives here
  google.protobuf.Timestamp create_time = 7;
  google.protobuf.Timestamp update_time = 8;
}

// Edge is a typed, directed relationship between two entities.
message Edge {
  string source_urn = 1;
  string target_urn = 2;
  string type = 3;              // "lineage", "owned_by", "has_column", "generated_by", ...
  google.protobuf.Struct properties = 4;  // optional edge metadata
  string source = 5;            // who reported this edge
}
```

### 3. Record wraps Entity + Edges (Go-only, no proto)

The `Record` type is a **Go struct** in Meteor, not a proto message. It's an internal pipeline envelope — it never gets serialized as-is. Sinks serialize the entity and edges separately.

```go
// models/record.go
package models

import (
    meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
)

type Record struct {
    entity *meteorv1beta1.Entity
    edges  []*meteorv1beta1.Edge
}

func NewRecord(entity *meteorv1beta1.Entity, edges ...*meteorv1beta1.Edge) Record {
    return Record{entity: entity, edges: edges}
}

func (r Record) Entity() *meteorv1beta1.Entity  { return r.entity }
func (r Record) Edges() []*meteorv1beta1.Edge    { return r.edges }
```

No proto wrapper needed. Record is pipeline glue, not a serialization format.

### 3. Field mapping: Asset → Entity

| v1beta2.Asset | Compass Entity/Edge | Where it went |
|---------------|---------------------|---------------|
| `urn` | `entity.urn` | Same |
| `name` | `entity.name` | Same |
| `type` | `entity.type` | Same |
| `service` | `entity.source` | Renamed |
| `description` | `entity.description` | Same |
| `data` (Any) | `entity.properties` (Struct) | Flattened, no typed wrapper |
| `labels` | `entity.properties.labels` | Merged into properties |
| `url` | `entity.properties.url` | Merged into properties |
| `owners` | edges (type=`owned_by`) | Became edges |
| `lineage.upstreams` | edges (type=`lineage`) | Became edges |
| `lineage.downstreams` | edges (type=`lineage`) | Became edges |
| `event` | Dropped | Rarely used, can go in properties if needed |
| `create_time` | `entity.created_at` | Same (proto field name differs) |
| `update_time` | `entity.updated_at` | Same (proto field name differs) |

### 4. Delete typed data schemas from proton

All 12 typed data schemas (`Table`, `Dashboard`, `Topic`, `Job`, `User`, `Group`, `Bucket`, `Application`, `Model`, `Metric`, `FeatureTable`, `Experiment`) and their supporting types (`Column`, `ColumnProfile`, `Chart`, `TopicProfile`, `Blob`, etc.) are **deleted** from `raystack/assets/v1beta2/`.

They served only the `anypb.Any` wrapper pattern. With `entity.properties` as a `Struct`, extractors build data as `map[string]interface{}` directly.

### 5. How extractors change

**Before (BigQuery table extractor):**
```go
table, _ := anypb.New(&v1beta2.Table{
    Columns: columns,
    Profile: profile,
    Attributes: attrs,
})
asset := &v1beta2.Asset{
    Urn:     urn,
    Name:    name,
    Service: "bigquery",
    Type:    "table",
    Data:    table,
    Labels:  labels,
    Owners:  owners,
    Lineage: &v1beta2.Lineage{Upstreams: upstreams},
}
emit(models.NewRecord(asset))
```

**After:**
```go
entity := models.NewEntity(urn, "table", name, "bigquery", map[string]interface{}{
    "columns": columns,
    "profile": profile,
    "labels":  labels,
})
var edges []*meteorv1beta1.Edge
for _, u := range upstreams {
    edges = append(edges, models.LineageEdge(u.Urn, urn, "bigquery"))
}
for _, o := range owners {
    edges = append(edges, models.OwnerEdge(urn, o.Email, "bigquery"))
}
emit(models.NewRecord(entity, edges...))
```

Key simplifications:
- No `anypb.New()` dance. Properties is a flat map.
- No separate Owners/Lineage fields. Everything is an edge.
- No proto import for typed schemas (Table, Dashboard, etc.).

### 6. How processors change

**Enrich processor:** Instead of `utils.GetAttributes(asset)` / `utils.SetAttributes(asset, ...)`, it reads/writes `entity.Properties` fields directly via `structpb` helpers.

**Labels processor:** Instead of modifying `asset.Labels`, it sets fields inside `entity.Properties`.

**Script processor:** Instead of `structmap.AsMap(asset)` with special Asset handling, it works on the protobuf Struct (which is already a map). Simpler conversion.

### 7. How sinks change

**Compass sink:** Thin mapper from Meteor's Entity/Edge to Compass's UpsertEntityRequest/UpsertEdgeRequest.
- `entity.urn/type/name/description/source/properties` → `UpsertEntityRequest` fields (1:1 today, may diverge)
- Lineage edges → `upstreams`/`downstreams` string arrays in `UpsertEntityRequest`
- `owned_by` edges → separate `UpsertEdge` calls
- No more `buildProperties`/`buildLineage`/`buildOwners` — the heavy conversion from Asset is gone

**Kafka sink:** `proto.Marshal(record.Entity())` + separate edge serialization, or serialize as JSON.

**File/Console/GCS sinks:** JSON serialize entity+edges via `protojson`.

**HTTP sink:** Marshal entity as JSON payload, optionally with script transform.

**Stencil sink:** Reads `entity.Properties["columns"]` instead of unmarshaling `Table` from `Asset.Data`.

**Frontier sink:** Reads `owned_by` edges from `record.Edges()` instead of `asset.GetOwners()`.

### 8. Helper utilities

```go
// models/builder.go — convenience for extractors

// NewEntity creates an entity with properties from a map.
func NewEntity(urn, typ, name, source string, props map[string]interface{}) *meteorv1beta1.Entity

// NewURN builds a URN string: "urn:{service}:{scope}:{type}:{id}"
func NewURN(service, scope, typ, id string) string

// LineageEdge creates a lineage edge.
func LineageEdge(sourceURN, targetURN, source string) *meteorv1beta1.Edge

// OwnerEdge creates an owned_by edge.
func OwnerEdge(entityURN, ownerEmail, source string) *meteorv1beta1.Edge

// ToJSON serializes a Record (entity + edges) to JSON.
func ToJSON(r Record) ([]byte, error)
```

## Execution Plan

### Step 1: Add proto to proton

**Repo:** `raystack/proton`

- Create `raystack/meteor/v1beta1/record.proto` with Entity and Edge messages (as specified in Design Decision #2)
- Do NOT delete `raystack/assets/v1beta2/` yet — other projects may reference it

### Step 2: Generate Go code in Meteor

**Repo:** `raystack/meteor`

- Update `buf.yaml` / `buf.gen.yaml` to generate from `raystack/meteor/v1beta1`
- Run `make generate-proto` → produces `models/raystack/meteor/v1beta1/*.pb.go`
- Delete `models/raystack/assets/v1beta2/` generated code

### Step 3: Rewrite models layer

- Rewrite `models/record.go` — Record wraps Entity + Edges (Go struct, not proto)
- Rewrite `models/util.go` — `ToJSON` uses protojson on Entity
- Add `models/builder.go` — `NewEntity`, `NewURN`, `LineageEdge`, `OwnerEdge` helpers
- Update `plugins/plugin.go` — interfaces stay the same (Emit, Process, Sink all use `models.Record`)

### Step 4: Rewrite all extractors

31 extractors, grouped by asset type for efficiency:

**Table extractors (16):** bigquery, bigtable, cassandra, clickhouse, couchdb, csv, elastic, mariadb, mongodb, mssql, mysql, oracle, postgres, presto, redshift, snowflake
- Replace `anypb.New(&v1beta2.Table{Columns: ...})` → properties map with `columns` key
- Replace `asset.Lineage` → lineage edges (bigquery only)
- Most are simple: URN + name + columns as properties

**Dashboard extractors (5):** grafana, metabase, redash, superset, tableau
- Replace `anypb.New(&v1beta2.Dashboard{Charts: ...})` → properties map with `charts` key
- Replace lineage (metabase, tableau) → lineage edges
- Replace owners (tableau) → owned_by edges

**User extractors (3):** frontier, github, gsuite
- Replace `anypb.New(&v1beta2.User{...})` → properties with user fields

**Other extractors (7):** kafka (topic), optimus (job), merlin (model), caramlstore (feature_table), gcs (bucket), application_yaml (application), http (varies)
- Each maps its typed proto fields to flat properties + edges

### Step 5: Rewrite all processors

**Enrich:** Read/write `record.Entity().Properties` instead of `utils.GetAttributes(asset)`
**Labels:** Set properties fields instead of `asset.Labels`
**Script:** Operate on protobuf Struct (already a map) instead of Asset → map → Asset round-trip

### Step 6: Rewrite all sinks

**Compass:** Thin pass-through — Entity → UpsertEntity, Edges → UpsertEdge
**Kafka:** Serialize entity proto (or JSON)
**File/Console/GCS:** JSON via protojson
**HTTP:** JSON payload, optionally script-transformed
**Stencil:** Read `entity.Properties["columns"]` directly
**Frontier:** Read owned_by edges from Record

### Step 7: Update agent/stream utilities

- `agent/batch.go` — no change (already uses `models.Record`)
- `agent/stream.go` — no change (already uses `models.Record`)
- `utils/custom_properties.go` — rewrite to work with `entity.Properties` or remove
- `structmap/` — simplify or remove (Struct is already a map)

### Step 8: Update tests

- Rewrite all extractor tests to assert Entity+Edges instead of Asset
- Rewrite processor tests
- Rewrite sink tests
- Update e2e tests

### Step 9: Update docs and examples

- Update all example recipes (recipe format doesn't change, but docs reference Asset)
- Update `docs/docs/reference/sinks.md`
- Update `docs/docs/concepts/`
- Update CLAUDE.md

### Step 10: Clean up proton (separate PR)

- Delete `raystack/assets/v1beta2/` from proton
- Delete `raystack/assets/v1beta1/` from proton
- Keep `raystack/compass/v1beta1/` (Compass's own API, unchanged)
- Keep `raystack/meteor/v1beta1/` (Meteor's own model)
- Future: evaluate converging Meteor and Compass protos if they remain identical

## Edge Type Conventions

Standard edge types that extractors should use:

| Edge Type | Meaning | Example |
|-----------|---------|---------|
| `lineage` | Data flows from source to target | airflow_task → bigquery_table |
| `owned_by` | Entity is owned by target | table → user |
| `has_column` | Table contains column | table → column (future) |
| `generated_by` | Entity was produced by target | table → job |
| `reads_from` | Entity reads from target | dashboard → table |
| `member_of` | Entity belongs to target group | user → group |

These are conventions, not enforced enums. Extractors can introduce new types freely (open type system).

## What This Enables

1. **Decoupled from Compass** — Meteor owns its model. The Compass sink is a thin mapper. Either side can evolve independently.
2. **Any sink gets entity+edges natively** — no sink-specific conversion logic.
3. **New entity types without proto changes** — add an extractor, pick a type string, done.
4. **Richer relationships** — extractors can emit `has_column`, `reads_from`, `generated_by` edges that Compass v2's graph can traverse.
5. **Simpler extractors** — no anypb.New dance, no proto imports for typed schemas.
6. **Clean Compass sink** — near 1:1 mapping to UpsertEntity/UpsertEdge API.
7. **Future: column-level lineage** — emit column entities + lineage edges between them.
