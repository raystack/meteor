# Processor

A recipe can have multiple processors registered. A processor is basically a function that:

- expects a list of data
- processes the list
- returns a list

The result from a processor will be passed on to the next processor until there is no more processor.

## Built-in Processors

### metadata

This processor will set and overwrite metadata with given fields in the config.

```yaml
name: sampe-recipe
processors:
  - name: metadata
    config:
      fieldA: valueA
      fieldB: valueB
```

More info about available processors can be found [here](../reference/processors.md).
