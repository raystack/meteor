# Introduction

Meteor is a plugin driven agent for collecting metadata. Meteor has plugins to source metadata from a variety of data stores, services and message queues. It also has sink plugins to send metadata to variety of third party APIs and catalog services.

Meteor agent uses recipes as a set of instructions which are configured by user. Recipes contains configurations about the source from which the metadata will be fetched, information about metadata processors and the destination to where the metadata will be sent.

Meteor’s plugin system allows new plugins to be easily added. With 50+ plugins and many more coming soon to extract and sink metadata, it is easy to start collecting metadata from various sources and sink to any data catalog or store.

## Key Features

- **No Dependency:** Written in Go. It compiles into a single binary with no external dependency.
- **Extensible:** Plugin system allows new sources and sinks to be easily added.
- **Ecosystem:** Extract metadata for many popular services with a wide number of service plugins.
- **Customizable:** Add your own processors and sinks to suit your many use cases.
- **Runtime:** Meteor can run inside VMs or containers with minimal memory footprint.

## Usage

Explore the following resources to get started with Meteor:

- [Usage Guides](./guides/introduction.md) will help you get started on Meteor.
- [Concepts](./concepts/overview.md) describes all important Meteor concepts.
- [Contribute](./contribute/contributing.md) contains resources for anyone who wants to contribute to Meteor.
