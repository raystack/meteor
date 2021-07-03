# Source

When the source field is defined, Meteor will extract data from a metadata source using details defined in the field. Extractor can be defined using the `type` field.

## Sample usage in a recipe
```yaml
name: sample-recipe
source:
 - type: kafka
   config:
     broker: broker:9092
```

More info about available extractors can be found [here](../guides/extractors.md).
