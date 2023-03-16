# Shield

Shield is a cloud-native role-based authorization-aware reverse-proxy service that helps you manage the authorization of given resources. With Shield, you can create groups and manage members, manage policies of the resources.

## Usage

```yaml
sinks:
  name: shield
  config:
    host: shield.com
    headers:
      X-Shield-Email: meteor@gotocompany.com
      X-Other-Header: value1, value2
```

## Contributing

Refer to the contribution guidelines for information on contributing to this module.