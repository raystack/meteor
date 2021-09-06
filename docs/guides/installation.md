# Installation

Meteor can be installed currently by one of the following ways:

## using brew

```sh
# install meteor (requires homebrew installed)
$ brew install odpf/taps/meteor

# Get info about meteor
$ meteor

# list down all the supported extractors, sinks, and processors
$ meteor list extractors
```

## using docker image

```bash
# pull
$ docker pull odpf/meteor

# Get info about commands
$ docker run --rm odpf/meteor

# list down all the extractors currently supported
$ docker run --rm odpf/meteor meteor list extractors
```
