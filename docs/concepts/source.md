# Source

When the source field is defined, Meteor will extract data from a metadata source using the details defined in the field. `type` field should define the name of Extractor you want, you can use one from this list [here](../reference/extractors.md). `config` of a extractor can be different for different Extractor and needs you to provide details to setup a connection between meteor and your source. To determine the required configurations you can visit README of each Extractor [here](../../plugins/extractors)

## Sample usage in a recipe

```yaml
name: sample-recipe
source:
 - type: kafka
   config:
     broker: broker:9092
```

More info about available extractors can be found [here](../reference/extractors.md).
