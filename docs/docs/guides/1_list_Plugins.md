# Plugins

Before getting started we expect you went through the [prerequisites](./introduction.md#prerequisites).

Meteor follows a plugin driven approach and hence includes basically three kinds of plugins for the metadata orchestration: extractors (source), processors, and sinks (destination).
Some details on these 3 are:

- **Extractors** are the set of plugins that are source of our metadata and include databases, dashboards, users, etc.

- **Processors** are the set of plugins that perform the enrichment or data processing for the metadata after extraction.

- **Sinks** are the plugins that act as the destination of our metadata after extraction and processing.

Read more about the concepts on each of these in [concepts](../concepts/overview.md).
To get more context on these plugins, it is recommended to try out the `list` command to get the list of plugins of a specific type. Commands to list the plugins are mentioned below

## Listing all the plugins

```bash
# list all available extractors
$ meteor list extractors

# list all extractors with alias 'e'
$ meteor list e

# list available sinks
$ meteor list sinks

# list all sinks with alias 's'
$ meteor list s

# list all available processors
$ meteor list processors

# list all processors with alias 'p'
$ meteor list p
```
