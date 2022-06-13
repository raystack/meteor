# Commands

Meteor currently supports the following commands and these can be utilised after the installation:

* completion: generate the auto completion script for the specified shell

* [gen](#creating-sample-recipes): The recipe will be printed on standard output.
Specify recipe name with the first argument without extension.
Use comma to separate multiple sinks and processors.

* [help](#get-help-on-commands-when-stuck): to help the user with meteor.

* [info](#getting-information-about-plugins): Info command is used to get suitable information about various plugins.
Specify the type of plugin as extractor, sink or processor.
Returns information like, sample config, output and brief description of the plugin.

* [lint](#linting-recipes): used for validation of the recipes.
Helps in avoiding any failure during running the meteor due to invalid recipe format.

* [list](#listing-all-the-plugins): used to state all the plugins of a certain type.

* [run](#running-recipes): the command is used for running the metadata extraction as per the instructions in the recipe.
Can be used to run a single recipe, a directory of recipes or all the recipes in the current directory.

## Listing all the plugins

```bash
# list all available extractors
$ meteor list extractors

# list all extractors with alias 'e'
$ meteor list e

# list available sinks
$ meteor list sinks

# list all sinks with alias 's'
$ meteor list s

# list all available processors
$ meteor list processors

# list all processors with alias 'p'
$ meteor list p
```

## Getting Information about plugins

```bash
# used to get info about different kinds of plugins
$ meteor info [plugin-type] <plugin-name>

# plugin-type can be sink, extractor or processor
$ meteor info sink console
$ meteor info processor enrich
$ meteor info extractor postgres
```

## Generating Sample recipe\(s\)

Since recipe is the main resource of Meteor, we first need to create it before anything else.
You can create a sample recipe using the gen command.

```bash
# generate a sample recipe
# generate a recipe with a bigquery extractor and a console sink
$ meteor gen recipe sample -e bigquery -s console

# generate recipe with multiple sinks
$ meteor gen recipe sample -e bigquery -s compass,kafka

# extractor(-e) as postgres, multiple sinks(-s) and enrich processor(-p)
# save the generated recipe to a recipe.yaml
meteor gen recipe sample -e postgres -s compass,kafka -p enrich > recipe.yaml
```

## Linting recipes

```bash
# validate specified recipes.
$ meteor lint recipe.yml

# lint all recipes in the specified directory
$ meteor lint _recipes/

# lint all recipes in the current directory
$ meteor lint .
```

## Running recipes

```bash
# run meteor for specified recipes
$ meteor run recipe.yml

# run all recipes in the specified directory
$ meteor run _recipes/

# run all recipes in the current directory
$ meteor run .
```

## get help on commands when stuck

```bash
# check for meteor help
$ meteor --help
```
