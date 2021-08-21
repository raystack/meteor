# Commands

List of available actions.

## Run a single recipe

```sh
# the source and sinks are expected to be running
$ meteor run ./sample-recipe.yaml
```

This command will run a single recipe using the given filepath.

## Run multiple recipes

```sh
# path-to-directory required and should contain all the recipes for your expected jobs
$ meteor rundir <path-to-directory>
```

This command will run all the recipes stored in the given path-to-recipes.
