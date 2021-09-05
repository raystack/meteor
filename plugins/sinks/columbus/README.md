# Columbus

Columbus is a search and discovery engine built for querying application deployments, datasets and meta resources. It can also optionally track data flow relationships between these resources and allow the user to view a representation of the data flow graph.

## Usage

```yaml
sinks:
  name: columbus
  config:
    host: https://columbus.com
	type: sample-columbus-type
	mapping:
	  new_fieldname: "json_field_name"
	  id: "resource.urn"
	  displayName: "resource.name"
```

## Contributing

Refer to the contribution guidelines for information on contributing to this module.