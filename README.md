# Meteor

![test workflow](https://github.com/goto/meteor/actions/workflows/test.yml/badge.svg)
![build workflow](https://github.com/goto/meteor/actions/workflows/build.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/goto/meteor)](https://goreportcard.com/report/github.com/goto/meteor)
[![Coverage Status](https://coveralls.io/repos/github/goto/meteor/badge.svg?branch=main)](https://coveralls.io/github/goto/meteor?branch=main)
[![Version](https://img.shields.io/github/v/release/goto/meteor?logo=semantic-release)](Version)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache)](LICENSE)

Meteor is a plugin driven agent for collecting metadata. Meteor has plugins to source metadata from a variety of data stores, services and message queues.
It also has sink plugins to send metadata to variety of third party APIs and catalog services.

<p align="center"><img src="./docs/static/assets/overview.svg" /></p>

## Key Features

- **No Dependency:** Written in Go. It compiles into a single binary with no external dependency.
- **Extensible:** Plugin system allows new sources and sinks to be easily added.
- **Ecosystem:** Extract metadata for many popular services with a wide number of service plugins.
- **Customizable:** Add your own processors and sinks to suit your many use cases.
- **Runtime:** Meteor can run inside VMs or containers with minimal memory footprint.

## Documentation

Explore the following resources to get started with Meteor:

- [Usage Guides](https://goto.github.io/meteor/docs/guides/introduction) will help you get started on Meteor.
- [Concepts](https://goto.github.io/meteor/docs/concepts/overview) describes all important Meteor concepts.
- [Contribute](https://goto.github.io/meteor/docs/contribute/guide) contains resources for anyone who wants to contribute to Meteor.

## Installation

Install Meteor on macOS, Windows, Linux, OpenBSD, FreeBSD, and on any machine.

#### Binary (Cross-platform)

Download the appropriate version for your platform from [releases](https://github.com/goto/meteor/releases) page. Once downloaded, the binary can be run from anywhere.
You don’t need to install it into a global location. This works well for shared hosts and other systems where you don’t have a privileged account.
Ideally, you should install it somewhere in your PATH for easy use. `/usr/local/bin` is the most probable location.

#### Homebrew

```sh
# Install meteor (requires homebrew installed)
$ brew install goto/tap/meteor

# Upgrade meteor (requires homebrew installed)
$ brew upgrade meteor

# Check for installed meteor version
$ meteor version
```

## Usage

Meteor’s CLI is fully featured but simple to use, even for those who have very limited experience working from the command line. Run `meteor --help` to see list of all available commands and instructions to use.

```sh
# List of commands
$ meteor --help

# Print command reference
$ meteor reference
```

## Running locally

```sh
# Clone the repo
$ git clone https://github.com/goto/meteor.git

# Install all the golang dependencies
$ go mod tidy

# Build meteor binary file
$ make build

# Run meteor on a recipe file
$ ./meteor run sample-recipe.yaml

# Run meteor on multiple recipes in a directory
$ ./meteor run directory-path
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

Read our [contributing guide](https://goto.github.io/meteor/docs/contribute/contributing) to learn about our development process, how to propose bugfixes and improvements, and how to build and test your changes to Meteor.

To help you get your feet wet and get you familiar with our contribution process, we have a list of [good first issues](https://github.com/goto/meteor/labels/good%20first%20issue) that contain bugs which have a relatively limited scope. This is a great place to get started.

This project exists thanks to all the [contributors](https://github.com/goto/meteor/graphs/contributors).

## License

Meteor is [Apache 2.0](LICENSE) licensed.
