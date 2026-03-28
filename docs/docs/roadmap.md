# Roadmap

Meteor started as a metadata collector. It did one thing well: pull metadata from data infrastructure and push it to a catalog. That was the right tool for the catalog era, where the consumer was a human browsing a UI.

The consumer has changed. AI agents, copilots, and autonomous systems now need to understand an organization's data landscape to do useful work. They need structured context, not catalog pages. They need a graph they can traverse, not a search index they can query. Meteor is already most of the way there. The extractors, the plugin system, the asset model, the lineage support — all of that is foundational. What's missing is richer extraction and smarter graph construction before the data reaches its destination.

This document describes where Meteor goes next.

## The Shift

The metadata catalog era was about **collecting and displaying**. The AI era is about **collecting and grounding**. The difference matters because it changes what needs to be extracted, how entities are resolved, what relationships are captured, and how fresh the data needs to be.

Today, Meteor extracts assets and pushes them to sinks as flat records. Each asset carries some lineage, some ownership, some schema information. But there is no entity resolution across sources. There is no cross-source relationship inference. The extraction model is batch-only, and the relationship vocabulary is limited to upstream/downstream lineage.

Meteor's job is to produce the richest, most connected, most current raw observations of an organization's metadata — and deliver them to Compass, which resolves entities, constructs the graph, indexes, and serves it. Meteor owns the supply side. Collection, enrichment, and delivery. Everything that happens before the data reaches the store. Entity resolution and graph construction belong in Compass, which has the full graph context needed to make those decisions.

## What Changes

### From Records to Graph

Meteor currently treats each extracted asset as an independent record flowing through a pipeline. The new model treats extraction as contributing nodes and edges to a unified graph.

This means:

- **Rich observations from every source.** The same logical table appears in BigQuery, dbt, Tableau, and Airflow. Meteor emits observations from each source with enough context — URNs, schemas, names, service identifiers — for Compass to resolve them into unified entities. Meteor doesn't need to know what's already in the graph. It delivers what it finds. Compass, which holds the full graph, handles entity resolution and deduplication.
- **Richer relationship types.** Lineage (upstream/downstream) is one relationship. Ownership, read-access, produced-by, documented-in, tested-by, derived-from — these all matter for AI reasoning. Extractors should capture these relationships at the source, and the asset model should carry them.
- **Temporal awareness.** Meteor should track what changed between extraction runs. Schema evolution, ownership transfers, new assets, removed assets. Delivering deltas rather than full snapshots makes the downstream graph fresher and the pipeline more efficient.

### From Data-Infra Only to Full Organizational Context

Meteor's current extractors cover databases, warehouses, BI tools, message queues, orchestrators, and cloud storage. That is the data infrastructure layer. For AI context to be genuinely useful, coverage needs to expand well beyond it.

- **Code and version control.** Repositories, pull requests, CI/CD pipelines. An AI agent debugging a data issue needs to know what code produces a table, when it last changed, and what the deploy pipeline looks like.
- **Documentation.** Confluence, Notion, internal wikis. The business context that explains why a table exists or what a metric means often lives outside the data stack.
- **API schemas.** OpenAPI specs, gRPC definitions. AI agents building integrations need to know what endpoints exist and what they accept.
- **Infrastructure topology.** Service dependencies, Kubernetes deployments, cloud resource relationships. The operational context that connects data assets to the systems that serve them.
- **Incidents and operations.** PagerDuty, OpsGenie, on-call schedules. When an AI agent is investigating a data quality issue, knowing that the upstream service had an incident last night is essential context.
- **Access and permissions.** Who can access what. AI agents need to respect and communicate boundaries.

Each new source type makes the context graph more complete. The extractor plugin system is Meteor's core strength — this is where it should invest most aggressively.

### From Batch Pull to Incremental and Event-Driven

Meteor runs on a batch schedule: execute a recipe, extract everything, push to sinks. For a context graph powering real-time AI agents, this model has limits.

- **Change detection.** Extractors should track watermarks and emit only what changed since the last run. This reduces load on sources, shrinks payloads, and enables faster refresh cycles.
- **Event receivers.** A lightweight webhook/event ingestion layer that can receive notifications — schema change deployed, incident opened, ownership transferred — and convert them into graph updates without a full extraction cycle. Not every source needs to be polled.
- **Incremental delivery.** Sinks should be able to push deltas rather than full snapshots. When Meteor detects that only 3 out of 500 tables changed, it should deliver 3 updates, not 500.

### From Flat Delivery to Rich Graph Delivery

Today, Meteor pushes individual asset records to sinks. Each record is self-contained. The sink doesn't get the big picture — it gets one asset at a time.

For Compass to build a proper graph, Meteor should deliver richer payloads:

- **Source-annotated observations.** Each observation carries its source identity, extraction timestamp, and enough context for Compass to perform entity resolution. Meteor doesn't resolve entities — it delivers the raw material for Compass to resolve them against the full graph.
- **Typed relationships.** Beyond upstream/downstream edges, Meteor should deliver the full set of relationships it discovered during extraction — ownership, read/write patterns, documentation links, test coverage.
- **Cross-source hints.** Some relationships only become visible when you see multiple sources together. A BigQuery table is produced by an Airflow job and consumed by a Tableau dashboard. Meteor can annotate observations with cross-source hints (matching URNs, shared identifiers) that help Compass infer and construct cross-source edges in the graph.

## Architecture Direction

The pipeline model stays. Extract, process, deliver. But the internals evolve:

```
Sources (extractors + event receivers)
          │
          v
    Enrichment Pipeline
    - relationship extraction
    - cross-source hint annotation
    - change detection
    - metadata enrichment
          │
          v
    Sinks
    - Compass (primary: rich observation delivery)
    - Kafka, GCS, HTTP (secondary: streaming, storage)
    - traditional sinks (backward compatible)
```

**Enrichment Pipeline** replaces the current flat record stream as the core processing layer. It enriches observations with typed relationships, cross-source hints, and change metadata. Entity resolution and graph construction happen in Compass, not here — Meteor's job is to deliver the richest possible raw observations.

**Event Receivers** complement extractors. Instead of only pulling metadata on a schedule, Meteor can also receive events and convert them into graph updates. A webhook from GitHub on a schema migration merge. A notification from PagerDuty when an incident opens. These become observations delivered to Compass without waiting for the next batch run.

**Compass as the primary sink.** While Meteor retains its multi-sink architecture, Compass becomes the primary destination. The delivery protocol between Meteor and Compass should evolve from flat asset upserts to rich observation payloads — typed relationships, cross-source hints, and change deltas. Compass receives these observations and handles entity resolution, deduplication, and graph construction.

## Priorities

Not all of this happens at once. The ordering reflects what delivers value fastest with the least disruption.

**First: Strengthen the observation model.** Enrich the relationship model beyond lineage. Deliver typed relationships and cross-source hints to Compass. Entity resolution happens in Compass — Meteor's job is to deliver rich, well-annotated observations. Everything else builds on this, and it can be done without breaking existing recipes.

**Second: Expand coverage.** Add extractors for code repositories, documentation systems, API schemas, and infrastructure topology. Each new source makes the context graph more complete and more valuable. This is where Meteor's plugin architecture pays off.

**Third: Go incremental.** Add change detection to existing extractors. Build the event receiver framework. Support delta delivery to sinks. This makes the graph fresher without increasing load.

**Fourth: Cross-source hints.** Build the enrichment layer that annotates observations with cross-source hints — shared identifiers, matching URNs, compatible schemas — that help Compass construct cross-source edges during entity resolution. This requires Meteor to see observations from multiple sources together to annotate effectively, but the actual graph construction and resolution happen in Compass.

## What Stays the Same

- **Single binary, no dependencies.** Meteor's operational simplicity is a feature.
- **Plugin architecture.** New extractors, processors, and sinks should still be easy to build and register.
- **Recipe-based configuration.** Recipes continue to work for defining extraction jobs. New modes (auto-discovery, event-driven) complement recipes, they don't replace them.
- **Existing extractors and sinks.** Everything that works today keeps working. The graph builder is an additive layer, not a replacement.

## Sink Strategy

Meteor currently ships nine sinks. With Compass as the primary graph store and serving layer, most of the sink surface becomes unnecessary. The investment goes into making the Compass sink richer, not into adding more destinations.

**Compass** is the primary sink. The delivery protocol evolves from flat asset upserts to rich graph payloads — resolved entities, typed relationships, change deltas, cross-source edges. This is where most of the sink investment goes. The sophistication is in what Meteor delivers to Compass, not in how many places it can deliver to.

**Kafka** stays for event streaming. Other systems need to react to metadata changes in real time — a governance tool notified when a new PII column appears, a cost system alerted when a new dataset is created, an observability platform tracking lineage shifts. These consumers don't want to poll Compass. They want events on a bus. Kafka becomes more valuable as Meteor moves to incremental extraction — instead of dumping the full catalog every run, Meteor publishes change events.

**Object storage** stays for archival and compliance. Raw metadata snapshots for audit trails, regulatory reviews, or historical analysis. "Show me what the schema looked like six months ago" or "prove we tracked PII lineage for this period." Compass keeps current state and some version history, but long-term archival is a different concern. The current GCS sink should generalize to support S3 and Azure Blob as well.

**HTTP** stays as the generic escape hatch. Someone always has a use case you didn't anticipate — a custom internal system, a third-party tool, a one-off migration. Rather than building a dedicated sink for every edge case, a configurable HTTP sink covers it.

**Console and File** stay for development and debugging. Zero cost to maintain, essential for local development and testing recipes.

**Frontier and Stencil** should be retired or frozen. They are tightly coupled to specific Raystack services and serve niche use cases that the HTTP sink can handle. Maintaining dedicated sinks for them is not justified going forward.

The principle is simple: Compass is the graph store, Kafka is the event bus, object storage is the archive, HTTP is the escape hatch. Everything else is either Compass's responsibility or too niche for Meteor to own.

## What Gets Simplified

- **Per-source isolation.** Today each recipe runs one source in isolation. For cross-source hint annotation, Meteor needs a mode where it can see observations from multiple sources together to annotate shared identifiers and matching URNs.
- **Manual enrichment.** Much of what processors do today — adding labels, enriching fields — should eventually be inferrable from the graph itself. The script processor remains for custom logic, but the common cases should be automatic.

## What Doesn't Belong in Meteor

Meteor is the collection and delivery layer. It does not own persistence, resolution, querying, or serving. Specifically:

- **Entity resolution and graph construction** belong in Compass. Compass has the full graph context needed to match incoming observations against existing entities, deduplicate, and construct the unified graph. Meteor delivers raw observations; Compass resolves them.
- **MCP server, context composition, and AI serving** belong in Compass. Compass is the always-on service with the full persisted graph. It is the natural interface for AI agents to query.
- **Semantic search and embeddings** belong in Compass. Indexing and retrieval are query-side concerns, not collection-side.
- **Change feeds and subscriptions** belong in Compass. Consumers subscribe to the store, not the pipeline.
- **Usage tracking, quality scoring, and impact analysis** belong in Compass. These require the full graph state and query history that only the persistent store has.

Meteor's job is to make Compass's graph as rich, connected, and current as possible. Compass's job is to make that graph useful. The boundary is delivery.

## The Bet

Every team building AI agents will need a way to ground those agents in organizational context. The metadata is scattered across dozens of systems. Someone needs to collect it, connect it, and deliver it in a form that a graph store can serve.

Meteor already knows how to collect. The next step is to enrich — richer extraction, typed relationships, cross-source hints, and incremental delivery. Entity resolution and graph construction happen in Compass, where the full context lives. Meteor's job is to deliver the richest possible raw material. That is the roadmap.
