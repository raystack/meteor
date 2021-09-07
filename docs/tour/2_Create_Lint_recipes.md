# Recipes - Creation and linting

A recipe is a set of instructions and configurations defined by user, and in Meteor they are used to define how a particular job will be performed.
Thus, for the entire set of orchestration all you will need to provide will be recipe\(s\) for all the jobs you want meteor to do.

Read more about the concepts of Recipe [here](../concepts/recipe.md).

A sample recipe can be generated using the commands mentioned [below](#generating-sample-recipes).
After making the necessary changes to the source, and sinks as per ypur local setup, you can validate the sample-recipe using steps mentioed [here](#linting-recipes).

## Generating Sample recipe\(s\)

```bash
# generate a sample recipe
# generate a recipe with a bigquery extractor and a console sink
$ meteor gen recipe sample -e <name-of-extractor> -s <single-or-multiple-sinks> -p <name-of-processors>

# command to generate recipe with multiple sinks
$ meteor gen recipe sample -e bigquery -s columbus,kafka

# for the tour you can use a single console sink
# extracor(-e) as postgres, sink(-s) and enrich processor(-p)
# save the generated recipe to a recipe.yaml
meteor gen recipe sample -e postgres -s console -p enrich > recipe.yaml
```

## Linting Recipe\(s\)

```bash
# validate specified recipes.
$ meteor lint recipe.yml
```

More options for lint and gen commands can be found [here](../reference/commands.md).
