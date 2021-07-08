# Guide

## Adding a new Extractor
Please follow this list when adding a new Extractor:
* Create unit test for the new extractor.
* If the source instance is required for testing, please include it in `docker-compose.yml` so others can easily run and test it.
* Also update `.github/workflows/test.yaml` to add the source as a service so test will not fail in CI (most likely it would be similar with what you put in `docker-compose.yml`).
* Update `docs/reference/extractors.md` with guide to use the new extractor.

## Adding a new Processor
Please follow this list when adding a new Processor:
* Create unit test for the new processor.
* If any service is required for testing, please include it in `docker-compose.yml` so others can easily run and test it.
* Also update `.github/workflows/test.yaml` to add the service so test will not fail in CI (most likely it would be similar with what you put in `docker-compose.yml`).
* Update `docs/reference/processors.md` with guide to use the new processor.

## Adding a new Sink
Please follow this list when adding a new Sink:
* Create unit test for the new processor.
* If any service is required for testing, please include it in `docker-compose.yml` so others can easily run and test it.
* Also update `.github/workflows/test.yaml` to add the service so test will not fail in CI (most likely it would be similar with what you put in `docker-compose.yml`).
* Update `docs/reference/sinks.md` with guide to use the new sink.
