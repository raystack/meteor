# Processor

A recipe can have none or many processors registered, depending upon how the user wants metadata to be processed. A processor is a function that takes each record, transforms it, and passes it to the next stage.

## How Processors Work

Processors execute **sequentially** in the order they are defined in the recipe. The output of one processor becomes the input of the next, forming a transformation pipeline:

```
Extractor → Processor 1 → Processor 2 → Processor 3 → Sink
```

If no processors are defined, records flow directly from the extractor to the sink unchanged.

Processors modify **entity properties** (name, description, labels, attributes, etc.). Edges (ownership, lineage) pass through processors unchanged -- they are handled by sinks at the end of the pipeline.

## Error Handling

If a processor encounters an error during execution, the entire recipe run fails. There is no skip-on-error behavior -- you must fix the processor configuration to resolve the issue.

## Built-in Processors

### Enrich

Append custom key-value attributes to each entity's data. Useful for adding metadata that is not present in the source system.

```yaml
processors:
  - name: enrich
    config:
      attributes:
        team: data-platform
        environment: production
```

### Labels

Append key-value labels to each entity. Labels are useful for categorization and filtering in downstream catalog services.

```yaml
processors:
  - name: labels
    config:
      labels:
        source: meteor
        classification: internal
```

### Script

Transform entities using a [Tengo](https://github.com/d5/tengo) script. The script processor gives you full control -- including the ability to make HTTP calls to external services for enrichment.

```yaml
processors:
  - name: script
    config:
      engine: tengo
      script: |
        asset.name = asset.name + " (processed)"
```

## Writing a Recipe with Processors

| key | Description | requirement |
| :--- | :--- | :--- |
| `name` | Name of the processor to use | required |
| `config` | Processor-specific configuration | required |

## Example: Chaining Multiple Processors

```yaml
processors:
  - name: enrich
    config:
      attributes:
        domain: payments
  - name: labels
    config:
      labels:
        source: meteor
  - name: script
    config:
      engine: tengo
      script: |
        asset.name = asset.name + " [" + asset.source + "]"
```

In this example, each entity first gets enriched with a `domain` attribute, then gets labeled with `source: meteor`, and finally has its name modified by the script processor.

More info about available processors can be found [here](../reference/processors.md).
