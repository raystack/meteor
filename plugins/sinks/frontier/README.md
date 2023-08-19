# Frontier

Frontier is a cloud-native role-based authorization-aware reverse-proxy service that helps you manage the authorization of given resources. With Frontier, you can create groups and manage members, manage policies of the resources.

## Usage

```yaml
sinks:
  name: frontier
  config:
    host: frontier.com
    headers:
      X-Frontier-Email: meteor@raystack.io
      X-Other-Header: value1, value2
```

## Contributing

Refer to the contribution guidelines for information on contributing to this module.
