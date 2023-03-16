# Guide

## Adding a new Extractor

Please follow this list when adding a new Extractor:

* Create unit test for the new extractor.
* Register your extractor [here](https://github.com/goto/meteor/tree/main/plugins/extractors/populate.go). This is also where you would inject any dependencies needed for your extractor.
* Create a markdown with your extractor details. \([example](https://github.com/goto/meteor/tree/main/plugins/extractors/mysql/README.md)\)
* Add your extractor to one of the extractor list in `docs/reference/extractors.md`.

## Adding a new Processor

Please follow this list when adding a new Processor:

* Create unit test for the new processor.
* If the source instance is required for testing, Meteor provides a utility to easily create a docker container to help with your test as shown [here](https://github.com/goto/meteor/tree/main/plugins/extractors/mysql/extractor_test.go#L35).
* Register your processor [here](https://github.com/goto/meteor/tree/main/plugins/processors/populate.go). This is also where you would inject any dependencies needed for your processor.
* Update `docs/reference/processors.md` with guide to use the new processor.

## Adding a new Sink

Please follow this list when adding a new Sink:

* Create unit test for the new processor.
* If the source instance is required for testing, Meteor provides a utility to easily create a docker container to help with your test as shown [here](https://github.com/goto/meteor/tree/main/plugins/extractors/mysql/extractor_test.go#L35).
* Register your sink [here](https://github.com/goto/meteor/tree/main/plugins/sinks/populate.go). This is also where you would inject any dependencies needed for your sink.
* Update `docs/reference/sinks.md` with guide to use the new sink.

