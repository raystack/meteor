# Contributing Guide

## Adding a new Extractor

Please follow this list when adding a new Extractor:

* Your extractor has to implement one of the [defined interfaces](../../proto/odpf/meta/data_models.md).
* Create unit test for the new extractor.
* Add [build tags](https://pkg.go.dev/go/build#hdr-Build_Constraints) `//+build integration` on top of your unit test file as shown [here](../../plugins/extractors/mysql/extractor_test.go). This would make sure the test will not be run on when we are testing all unit tests.
* If the source instance is required for testing, Meteor provides a utility to easily create a docker container to help with your test as shown [here](../../plugins/extractors/mysql/extractor_test.go#L35).
* Register your extractor [here](../../plugins/extractors/populate.go). This is also where you would inject any dependencies needed for your extractor.
* Create a markdown with your extractor details. ([example](../../plugins/extractors/mysql/README.md))
* Add your extractor to one of the extractor list in `docs/reference/extractors.md`.

## Adding a new Processor

Please follow this list when adding a new Processor:

* Create unit test for the new processor.
* If the source instance is required for testing, Meteor provides a utility to easily create a docker container to help with your test as shown [here](../../plugins/extractors/mysql/extractor_test.go#L35).
* Register your processor [here](../../plugins/processors/populate.go). This is also where you would inject any dependencies needed for your processor.
* Update `docs/reference/processors.md` with guide to use the new processor.

## Adding a new Sink

Please follow this list when adding a new Sink:

* Create unit test for the new processor.
* If the source instance is required for testing, Meteor provides a utility to easily create a docker container to help with your test as shown [here](../../plugins/extractors/mysql/extractor_test.go#L35).
* Register your sink [here](../../plugins/sinks/populate.go). This is also where you would inject any dependencies needed for your sink.
* Update `docs/reference/sinks.md` with guide to use the new sink.
