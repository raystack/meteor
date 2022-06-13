# Recipes - Creation and linting

A recipe is a set of instructions and configurations defined by the user, and in Meteor they are used to define how a particular job will be performed.
Thus, for the entire set of orchestration all you will need to provide will be recipe\(s\) for all the jobs you want meteor to do.

Read more about the concepts of Recipe [here](../concepts/recipe.md).

A sample recipe can be generated using the `meteor new` command mentioned [below](#generating-new-sample-recipes).

One can also generate multiple recipes with similar configurations using the `meteor gen` command mentioned [below](#generating-multiple-recipes-from-a-template).

After making the necessary changes to the source, and sinks as per your local setup, you can validate the sample-recipe using steps mentioned [here](#linting-recipes).

## Generating new Sample recipe\(s\)

```bash
# generate a sample recipe
# generate a recipe with a bigquery extractor and a console sink
$ meteor new recipe sample -e <name-of-extractor> -s <single-or-multiple-sinks> -p <name-of-processors>

# command to generate recipe with multiple sinks
$ meteor new recipe sample -e bigquery -s compass,kafka

# for the tour you can use a single console sink
# extractor(-e) as postgres, sink(-s) and enrich processor(-p)
# save the generated recipe to a recipe.yaml
$ meteor new recipe sample -e postgres -s console -p enrich > recipe.yaml

# if not sure about the list of plugins you can choose from
# the cli is interactive
$ meteor run recipe sample
```

## Generating multiple recipes from a template

Usually it may be required by user to generate multiple recipes with similar configuration, and just small variable.

```bash
# generate multiple recipes with same template
$ meteor run  template.yaml --d <templates-data> --o <output-directory>
```

## Linting Recipe\(s\)

```bash
# validate specified recipes.
$ meteor lint recipe.yaml
```

More options for lint and gen commands can be found [here](../reference/commands.md).
