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

var (
	ErrInvalidRecipeVersion = errors.New("recipe version is invalid or not found")
)

// NewReader returns a new Reader.
func NewReader(lg log.Logger, pathToConfig string) *Reader {
	reader := &Reader{}
	reader.data = populateData(pathToConfig)
	reader.log = lg
	return reader
}

// Read loads the list of recipes from a give file or directory path.
func (r *Reader) Read(path string) (recipes []Recipe, err error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	switch mode := fi.Mode(); {
	case mode.IsDir():
		recipes, err = r.readDir(r.log, path)
		if err != nil {
			return nil, err
		}
	case mode.IsRegular():
		recipe, err := r.readFile(path)
		if err != nil {
			return nil, err
		}
		recipes = append(recipes, recipe)
	}
	return
}

func (r *Reader) readFile(path string) (recipe Recipe, err error) {
	template, err := template.ParseFiles(path)
	if err != nil {
		return
	}

	var buff bytes.Buffer
	err = template.Execute(&buff, r.data)
	if err != nil {
		return
	}

	var node RecipeNode
	err = yaml.Unmarshal(buff.Bytes(), &node)
	if err != nil {
		return
	}

	if node.Name.Value == "" {
		file := filepath.Base(path)
		fileName := strings.TrimSuffix(file, filepath.Ext(file))
		node.Name.Value = fileName
	}

	versions := generator.GetRecipeVersions()
	err = validateRecipeVersion(node.Version.Value, versions[len(versions)-1])
	if err != nil {
		return
	}

	recipe, err = node.toRecipe()
	if err != nil {
		return
	}

	return
}

func (r *Reader) readDir(lg log.Logger, path string) (recipes []Recipe, err error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return
	}

	for _, entry := range entries {
		x := filepath.Join(path, entry.Name())
		recipe, err := r.readFile(x)
		if err != nil {
			lg.Warn("skipping file", "path", x, "err", err.Error())
			continue
		}

		recipes = append(recipes, recipe)
	}

	return
}

func validateRecipeVersion(receivedVersion, expectedVersion string) (err error) {
	if strings.Compare(receivedVersion, expectedVersion) == 0 {
		return
	}
	return ErrInvalidRecipeVersion
}
