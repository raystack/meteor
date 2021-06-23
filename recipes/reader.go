package recipes

import (
	"io/ioutil"

	"github.com/odpf/meteor/domain"
	"gopkg.in/yaml.v3"
)

type Reader struct {
}

func NewReader() *Reader {
	return &Reader{}
}

func (r *Reader) Read(path string) (recipe domain.Recipe, err error) {
	recipeBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return recipe, err
	}

	err = yaml.Unmarshal(recipeBytes, &recipe)
	if err != nil {
		return recipe, err
	}

	return recipe, err
}
