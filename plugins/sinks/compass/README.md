# Compass

Compass is a search and discovery engine built for querying application deployments, datasets and meta resources. It can also optionally track data flow relationships between these resources and allow the user to view a representation of the data flow graph.

## Usage

```yaml
sinks:
  name: compass
  config:
    host: https://compass.com
    headers:
      compass-User-Email: meteor@gotocompany.com
      Header-1: value11,value12
    labels:
      myCustom: $properties.attributes.myCustomField
      sampleLabel: $properties.labels.sampleLabelField
    remove_unset_fields_in_data: false
```
### *Notes*

- Setting `remove_unset_fields_in_data` to `true` will not populate fields in final data which are not set initially in source. Defaults to `false`.
## Contributing

Refer to the [contribution guidelines](../../../docs/docs/contribute/guide.md#adding-a-new-sink) for information on contributing to this module.
