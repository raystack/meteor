# Usage

This section assumes you already have Meteor installed. If not, you can find how to do it [here](installation.md).
Meteor is based out on the plugins approach and hence includes basically three kinds of plugins for the metadata orchestration: extractors (source), processors, and sinks (destination).
Extractors are the set of plugins that are source of our metadata and include databases, dashboards, users, etc.
Processors are the set of plugins that perform the enrichment or data processing for the metadata after extraction.
Sinks are the plugins that act as the destination of our metadata after extraction and processing.
Read more about the concepts on each of these in [concepts](../concepts/README.md).

## List of available commands

Meteor currently supports the following commands and these can be utilised after the installation:

* completion: generate the autocompletion script for the specified shell

* [gen](#creating-sample-recipes): ```meteor gen recipe <filename> [flags]```

* help: ```meteor help```

* info: ```meteor info <plugins-type> <plugin-name>```

* lint: ```meteor lint [path]```

* list: ```meteor list <command> [flags]```

* run: ```meteor run sample-recipe.yaml```

## Generating Sample recipe\(s\)

Since recipe is the main resource of Meteor, we first need to create it before anything else.
You can create a sample recipe usin the gen command.

```bash
#generate a sample recipe.yaml file for extracor as postgres, multiple sinks and enrich processor.
# save the generated recipe to a recipe.yaml
meteor gen recipe sample -e postgres -s columbus,kafka -p enrich > recipe.yaml
```

Then edit the recipe file using [this guide](../concepts/recipe.md).

## Running a single recipe

Once we have a recipe. We can easily run it using below command.

```bash
# sample-recipe.yaml should be valid, please refer concepts/recipe.md
$ meteor run sample-recipe.yaml
```

This will run the recipe using its details. More information about the command can be found [here](../reference/commands.md#run-a-single-recipe).

## Running multiple recipes from directory

One can store all his recipes in a single directory and know our `path-to-recipes`, which is the path to the directory. We can easily run it using below command.

```bash
# path-to-recipes contains path to directory e=with all the recipes
$ meteor rundir <path-to-recipes>
```

This will run the recipe using its details. More information about the command can be found [here](../reference/commands.md#run-a-single-recipe).
