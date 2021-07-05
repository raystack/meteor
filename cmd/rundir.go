package cmd

import (
	"fmt"

	"github.com/odpf/meteor/config"
	"github.com/odpf/meteor/recipes"
)

// run recipes in a given directory path
func rundir(dirPath string) {
	c, err := config.LoadConfig()
	if err != nil {
		panic(err)
	}

	reader := recipes.NewReader()
	recipeList, err := reader.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}

	runner := initRunner(c)
	if err != nil {
		panic(err)
	}
	faileds, err := runner.RunMultiple(recipeList)
	if err != nil {
		panic(err)
	}

	fmt.Println("Failed recipes:")
	fmt.Printf("%+v\n", faileds)
}
