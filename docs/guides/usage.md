# Usage

This section assumes you already have Meteor installed. If not, you can find how to do it [here](installation.md).

## Creating recipe\(s\)

Since recipe is the main resource of Meteor, we first need to create it before anything else.

```bash
#creates a recipe file in current directory
$ touch sample-recipe.yaml
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

