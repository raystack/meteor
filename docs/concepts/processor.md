# Processor

A recipe can have none or many processors registered, depending upon the way user want metadata to processed. A processor is basically a function that:

* expects a list of data
* processes the list
* returns a list

The result from a processor will be passed on to the next processor until there is no more processor, hence the flow is sequential.

## Built-in Processors

### metadata

This processor will set and overwrite metadata with given fields in the config.

```yaml
processors:
  - name: metadata
    config:
      fieldA: valueA
      fieldB: valueB
```

| key | Description | requirement |
| :--- | :--- | :--- |
| `name` | contains the name of processor | required |
| `config` | different processors will require different config | required |

More info about available processors can be found [here](../reference/processors.md).

