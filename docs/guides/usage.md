# Usage

This section assumes you already have Meteor installed. If not, you can find how to do it [here](installation.md).
Meteor is based out on the plugins approach and hence includes basically three kinds of plugins for the metadata orchestration: extractors (source), processors, and sinks (destination).
Extractors are the set of plugins that are source of our metadata and include databases, dashboards, users, etc.
Processors are the set of plugins that perform the enrichment or data processing for the metadata after extraction.
Sinks are the plugins that act as the destination of our metadata after extraction and processing.
Read more about the concepts on each of these in [concepts](../concepts/README.md).
