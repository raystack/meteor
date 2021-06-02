package recipes

import (
	"io/ioutil"

	"github.com/odpf/meteor/domain"
	"gopkg.in/yaml.v3"
)

type Reader struct {
	Dirname string
}

func (rr *Reader) Read() (recipes []domain.Recipe, err error) {
	yamls, err := rr.getFilesFromDir()
	if err != nil {
		return recipes, err
	}

	recipes, err = rr.parseYAMLsToRecipes(yamls)
	if err != nil {
		return recipes, err
	}

	return recipes, nil
}

func (rr *Reader) getFilesFromDir() (yamls [][]byte, err error) {
	files, err := ioutil.ReadDir(rr.Dirname)
	if err != nil {
		return yamls, err
	}

	for _, f := range files {
		yamlFile, err := ioutil.ReadFile(rr.Dirname + "/" + f.Name())
		if err != nil {
			return yamls, err
		}

		yamls = append(yamls, yamlFile)
	}

	return yamls, err
}

func (rr *Reader) parseYAMLsToRecipes(yamls [][]byte) (recipes []domain.Recipe, err error) {
	for _, yamlFile := range yamls {
		var recipe domain.Recipe
		err = yaml.Unmarshal(yamlFile, &recipe)
		if err != nil {
			return recipes, err
		}

		recipes = append(recipes, recipe)
	}

	return recipes, err
}

func NewReader(dirname string) *Reader {
	return &Reader{
		Dirname: dirname,
	}
}
