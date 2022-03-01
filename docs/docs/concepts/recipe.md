# Recipe

A recipe is a set of instructions and configurations defined by user, and in Meteor they are used to define how a particular job will be performed. It should contain instruction about the `source` from which the metadata will be fetched, information about metadata `processors` and the destination is to be defined as `sinks` of metadata.

The recipe should contain about **only one** `source` since we wish to have a seperate job for different extractors, hence keeping them isolated. Should have **atleast one** destination of metadata mentioned in `sinks`, and the `processors` field is optional but can have multiple processors.

Recipe is a yaml file, follows a structure as shown below and needs to passed as a individual file or as a bunch of recipes contained in a directory as shown in [sample usage](recipe.md#sample-usage).

## Writing a Recipe for Meteor

* _sample-recipe.yaml_

```yaml
name: main-kafka-production # unique recipe name as an ID
source: # required - for fetching input from sources
 name: kafka # required - collector to use (e.g. bigquery, kafka)
 config:
   broker: "localhost:9092"
sinks: # required - at least 1 sink defined
  - name: http
    config:
      method: POST
      url: "https://example.com/metadata"
  - name: console
processors: # optional - metadata processors
  - name: metadata
    config:
      foo: bar
      bar: foo
```

### Glossary Table

Contains details about the ingridients of our recipe. The `config` of each source, sinks and processors differs as different data source required different kinds of credentials, please refer more about them in further reference section.

| Key | Description | Requirement | further reference |
| :--- | :--- | :--- | :--- |
| `name` | **unique** recipe name, will be used as ID for job | required | N/A |
| `source` | contains details about the source of metadata extraction | required | [source](source.md) |
| `sinks` | defines the final destination's of extracted and processed metadata | required | [sink](sink.md) |
| `processors` | used process the metadata before sinking | optional | [processor](processor.md) |

## Dynamic recipe value

Meteor reads recipe using [go template](https://golang.org/pkg/text/template/), which means you can put a variable instead of static value in a recipe.
Environment variables with prefix `METEOR_`, such as `METEOR_MONGODB_PASS`, will be used as the template data for the recipe.
This is to allow you to skip creating recipes containing the credentials of datasource.

* _recipe-with-variable.yaml_

```yaml
name: sample-recipe
source:
  name: mongodb
  config:
    # wrap it with double quotes to make sure value is read as a string
    connection_url: "{{ .connection_url }}"
sinks:
  - name: http
    config:
      method: POST
      url: "https://example.com/metadata"
```

## Sample Usage

```text
#setup environment variables
> export METEOR_CONNECTION_URL=mongodb://admin:pass123@localhost:3306
#run a single recipe
> meteor run recipe-with-variable.yaml
#run multiple recipes contained in single directory
> meteor rundir path/directory-of-recipes
```

## Support to pass secrets through .env file

Meteor allows you to maintain a `.env` file as well which can be used as a template data for recipe.
The variables here should not contain a `METEOR_` prefix and should be as normal as any other `.env` file.
Meteor reads both local environment variables as well as the ones from `.env` file and in case of conflict prefers the one mentioned in `.env` file.

* _.env_

```env
CONNECTION_URL=mongodb://admin:pass123@localhost:3306
```
