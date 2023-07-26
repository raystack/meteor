# Installation

Meteor can be installed currently by one of the following ways:

## Using HomeBrew

```sh
# install meteor (requires homebrew installed)
$ brew install raystack/tap/meteor

# Get info about meteor
$ meteor

# list down all the supported extractors, sinks, and processors
$ meteor list extractors
```

## Binary from releases

The binaries are downloadable from the [Github releases][github-releases] page.
There is currently no installer available.
You have to add the meteor binary to the `PATH` environment variable yourself or put the binary in a location that is already in your `$PATH` (e.g. /usr/local/bin, ...).

Once installed, you should be able to run:

`$ meteor version`

## Using docker image

```bash
# pull
$ docker pull raystack/meteor

# Get info about commands
$ docker run --rm raystack/meteor

# list down all the extractors currently supported
$ docker run --rm raystack/meteor meteor list extractors
```

## Build from source

Requires you to have `git` and `golang (version 1.16 or above)` installed.

```bash
#clone repo
$ git clone https://github.com/raystack/meteor.git

$ cd meteor

$ make build

$ ./meteor --help
```

[github-releases]: https://github.com/raystack/meteor/releases
