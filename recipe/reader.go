package recipe

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/goto/meteor/generator"
	"github.com/goto/salt/log"
	"gopkg.in/yaml.v3"
)

// Reader is a struct that reads recipe files.
type Reader struct {
	data map[string]string
	log  log.Logger
}

var ErrInvalidRecipeVersion = errors.New("recipe version is invalid or not found")

// NewReader returns a new Reader.
func NewReader(lg log.Logger, pathToConfig string) *Reader {
	reader := &Reader{}
	reader.data = populateData(pathToConfig)
	reader.log = lg
	return reader
}

// Read loads the list of recipes from a give file or directory path.
func (r *Reader) Read(path string) ([]Recipe, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	switch mode := fi.Mode(); {
	case mode.IsDir():
		return r.readDir(r.log, path)

	case mode.IsRegular():
		recipe, err := r.readFile(path)
		if err != nil {
			return nil, err
		}

		return []Recipe{recipe}, nil
	}

	return nil, nil
}

func (r *Reader) readFile(path string) (Recipe, error) {
	tmpl, err := template.ParseFiles(path)
	if err != nil {
		return Recipe{}, err
	}

	var buff bytes.Buffer
	if err := tmpl.Execute(&buff, r.data); err != nil {
		return Recipe{}, err
	}

	var node RecipeNode
	if err := yaml.Unmarshal(buff.Bytes(), &node); err != nil {
		return Recipe{}, err
	}

	if node.Name.Value == "" {
		file := filepath.Base(path)
		fileName := strings.TrimSuffix(file, filepath.Ext(file))
		node.Name.Value = fileName
	}

	versions := generator.GetRecipeVersions()
	err = validateRecipeVersion(node.Version.Value, versions[len(versions)-1])
	if err != nil {
		return Recipe{}, err
	}

	recipe, err := node.toRecipe()
	if err != nil {
		return Recipe{}, err
	}

	return recipe, nil
}

func (r *Reader) readDir(lg log.Logger, path string) ([]Recipe, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var recipes []Recipe
	for _, entry := range entries {
		x := filepath.Join(path, entry.Name())
		recipe, err := r.readFile(x)
		if err != nil {
			lg.Warn("skipping file", "path", x, "err", err.Error())
			continue
		}

		recipes = append(recipes, recipe)
	}

	return recipes, nil
}

func validateRecipeVersion(receivedVersion, expectedVersion string) error {
	if receivedVersion != expectedVersion {
		return ErrInvalidRecipeVersion
	}
	return nil
}
