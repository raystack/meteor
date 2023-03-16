# Running Meteor

After we are done with creating sample recipe or a folder of sample recipes.
We move ahead with the process of metadata extraction with meteor.
Follow the following commands to run the recipes:

## with meteor binary installed

```bash
# run meteor for specified recipes
$ meteor run recipe.yml

# run all recipes in the specified directory
$ meteor run _recipes/

# run all recipes in the current directory
$ meteor run .
```

## with docker image pulled locally

```bash
# run meteor for specified recipes
$ docker run --rm gotocompany/meteor meteor run recipe.yml

# run all recipes in the specified directory
$ docker run --rm gotocompany/meteor meteor run _recipes/

# run all recipes in the current directory
$ docker run --rm gotocompany/meteor meteor run .
```
