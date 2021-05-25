# Meteor

![test workflow](https://github.com/odpf/meteor/actions/workflows/test.yml/badge.svg)
![build workflow](https://github.com/odpf/meteor/actions/workflows/build.yml/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache)](LICENSE)
[![Version](https://img.shields.io/github/v/release/odpf/meteor?logo=semantic-release)](Version)

Meteor is a metadata collector service that helps to extract and sink metadata from the source to the destination.

### Installation

#### Compiling from source

It requires the following dependencies:

* Docker
* Golang (version 1.14 or above)
* Git

Run the application dependecies using Docker:

Update the configs(host etc.) as per your dev machine and docker configs.

Run the following commands to compile from source

```
$ git clone git@github.com:odpf/meteor.git
$ make build
```

To run tests locally

```
$ make test
```

To run tests locally with coverage

```
$ make test-coverage
```

To run server locally

```
$ go run .
```

#### Config

The config file used by application is `config.yaml` which should be present at the root of this directory.

For any variable the order of precedence is:

1. Env variable
2. Config file
3. Default in Struct defined in the application code

For list of all available configuration keys check the [configuration](docs/reference/configuration.md) reference.
