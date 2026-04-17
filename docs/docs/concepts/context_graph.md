# Context Graph for AI

AI systems — large language models, copilots, autonomous agents — are powerful reasoners but poor guessers. Without grounding in real metadata, they hallucinate table names, fabricate schemas, and miss relationships between systems. The gap between what AI can do and what it actually knows about your data landscape is the **context gap**.

Meteor closes this gap. By continuously extracting metadata — schemas, lineage, ownership, descriptions — from across your data infrastructure, Meteor builds the structured knowledge layer that AI needs to reason accurately. This layer is the **context graph**.

## What is a Context Graph?

A context graph is a connected, queryable representation of your data ecosystem. It captures not just what assets exist, but how they relate to each other:

- **Nodes** represent assets: tables, dashboards, jobs, topics, models, buckets, users, repositories, teams, groups.
- **Edges** represent relationships: lineage (which table feeds which dashboard), ownership (who is responsible), membership (who belongs to which team), and dependency (which job produces which dataset).

Unlike a flat catalog or a search index, a context graph preserves **structure**. It knows that a revenue dashboard depends on a sales table, which is produced by an ETL job, which reads from a Kafka topic. This structure is what makes AI useful over enterprise data.

## How Meteor Builds the Context Graph

Meteor operates as a metadata supply chain with three stages:

### Extract

Meteor's extractors connect to 30+ data sources — databases (BigQuery, Postgres, Snowflake), BI tools (Tableau, Metabase, Superset), streaming platforms (Kafka), cloud storage (GCS), orchestrators (Optimus), and more. Each extractor produces **Record**s — an **Entity** with flat properties plus **Edge**s representing relationships:

- **Schema metadata** — column names, types, descriptions, constraints (stored as entity properties)
- **Lineage** — upstream and downstream relationships (represented as edges)
- **Ownership** — who owns and maintains the entity (represented as edges)
- **Service context** — source system, URLs, timestamps

### Process

Processors enrich and transform assets in-flight before they reach a sink. Use them to:

- Append **labels** for classification (environment, domain, sensitivity, PII)
- **Enrich** assets with custom fields from external systems
- Run **scripts** (Tengo) for arbitrary transformation logic, including HTTP calls to external APIs

### Deliver

Sinks push the enriched metadata to wherever your AI systems can consume it — a metadata catalog (Compass), a search index, cloud storage (GCS), a streaming platform (Kafka), or a generic HTTP endpoint.

```
Sources (30+)          Processors           Sinks
┌──────────────┐      ┌──────────┐      ┌────────────────┐
│ BigQuery     │      │ Labels   │      │ Compass        │
│ Postgres     │─────▶│ Enrich   │─────▶│ Kafka          │
│ Tableau      │      │ Script   │      │ GCS / HTTP     │
│ Kafka  ...   │      └──────────┘      └────────────────┘
└──────────────┘                              │
                                              ▼
                                        Context Graph
                                              │
                                              ▼
                                     AI Systems & Agents
```

## Why AI Needs a Context Graph

### Grounding and Retrieval (RAG)

Retrieval-Augmented Generation depends on having a rich, accurate corpus to search against. Meteor-extracted metadata — table descriptions, column names, business labels, ownership — becomes the retrieval layer that grounds LLM responses in reality.

When a user asks *"find me all tables related to revenue"*, the AI searches over Meteor-extracted descriptions and labels instead of guessing. When it generates SQL, it uses real column names and types from the schema metadata.

### Lineage as Causal Reasoning

Most metadata systems tell AI **what exists**. Lineage tells it **how things connect**. This is the difference between a lookup tool and a reasoning partner.

With Meteor's lineage graph, AI can:

- **Trace impact** — "If I change this table's schema, which dashboards break?"
- **Root-cause analysis** — "This metric dropped. What changed upstream?"
- **Dependency awareness** — "Before deprecating this dataset, show me everything downstream."

### Asset Discovery for AI Agents

Function-calling AI agents need to know what tools and data are available. The context graph serves as the agent's **world model**:

- What tables, APIs, dashboards, and services exist
- What columns and types are available for SQL generation
- How services connect (topic → consumer → table → dashboard)
- Who to escalate to when something goes wrong

### Trust and Data Quality Signals

AI should not treat all data equally. Metadata enriched through Meteor's processors can carry trust signals — freshness, ownership, sensitivity labels, environment tags — that help AI systems prioritize reliable, appropriate data sources over stale or restricted ones.

## Extending Meteor for AI Workloads

Meteor's plugin architecture makes it straightforward to extend the context graph for AI-specific use cases:

| Capability | Approach |
|---|---|
| **Semantic search** | Use a script processor to generate vector embeddings from asset descriptions, enabling similarity-based retrieval |
| **Business glossary** | Extract metric definitions and business terms as first-class assets, linking them to underlying tables |
| **Usage signals** | Build extractors that capture query frequency and dashboard views, helping AI rank assets by relevance |
| **Data quality** | Enrich assets with freshness, completeness, and anomaly scores so AI can assess data trustworthiness |
| **LLM-optimized exports** | Create sinks that format metadata as structured context windows sized for LLM consumption |

## The Flywheel

The context graph is not a one-time build. It is a continuously improving loop:

1. **Meteor extracts** metadata from across the data ecosystem
2. **The context graph** grows richer with each extraction cycle
3. **AI systems** use the graph for grounding, reasoning, and discovery
4. **AI interactions** reveal gaps — missing descriptions, unknown lineage, unlabeled assets
5. **Teams fill gaps**, improving metadata quality
6. **Meteor captures** the improvements, and the cycle continues

Each iteration makes the AI more capable and the metadata more complete. Meteor is the engine that keeps this flywheel turning.
