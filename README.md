# Meteor

![test workflow](https://github.com/odpf/meteor/actions/workflows/test.yml/badge.svg)
![build workflow](https://github.com/odpf/meteor/actions/workflows/build.yml/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache)](LICENSE)
[![Version](https://img.shields.io/github/v/release/odpf/meteor?logo=semantic-release)](Version)

Meteor is a metadata collector tool that helps to extract and sink metadata from the source to the destination.

<p align="center"><img src="./docs/assets/overview.svg" /></p>

## Key Features

* **Metadata Extractions** Easily orchestrate your metadata extraction via recipe and Meteor's built-in features.
* **Scale:** Meteor scales in an instant, both vertically and horizontally for high performance.
* **Customizable:** Add your own processors and sinks to suit your many use cases.
* **Runtime:** Meteor can run inside VMs or containers in a fully managed runtime environment like kubernetes.

## Usage

Explore the following resources to get started with Meteor:

* [Usage Guides](docs/guides/usage.md) will help you get started on Meteor.
* [Concepts](docs/concepts) describes all important Meteor concepts.
* [Contribute](docs/contribute/contributing.md) contains resources for anyone who wants to contribute to Meteor.

## Running locally

```sh
# Clone the repo
$ git clone https://github.com/odpf/meteor.git

# Install all the golang dependencies
$ go mod tidy

# Build meteor binary file
$ make build

# Run meteor on a recipe file
$ ./meteor run sample-recipe.yaml

# Run meteor on multiple recipes in a directory
$ ./meteor rundir directory-path
```

## Running tests

```sh
# Running all unit tests, excluding extractors
$ make test

# Run integration test for any extractor
$ cd plugins/extractors/<name-of-extractor>
$ go test -tags=integration
```

## Contribute

Development of Meteor happens in the open on GitHub, and we are grateful to the community for contributing bugfixes and improvements. Read below to learn how you can take part in improving Meteor.

Read our [contributing guide](docs/contribute/contributing.md) to learn about our development process, how to propose bugfixes and improvements, and how to build and test your changes to Meteor.

To help you get your feet wet and get you familiar with our contribution process, we have a list of [good first issues](https://github.com/odpf/meteor/labels/good%20first%20issue) that contain bugs which have a relatively limited scope. This is a great place to get started.

This project exists thanks to all the [contributors](https://github.com/odpf/meteor/graphs/contributors).

## License

Meteor is [Apache 2.0](LICENSE) licensed.
