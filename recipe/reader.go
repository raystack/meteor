package recipe

import (
	"bytes"
	"os"
	"path/filepath"
	"text/template"

	"gopkg.in/yaml.v3"
)

// Reader is a struct that reads recipe files.
type Reader struct {
	data map[string]string
}

// NewReader returns a new Reader.
func NewReader() *Reader {
	reader := &Reader{}
	reader.data = populateData()

	return reader
}

//  Read loads the list of recipes from a give file or directory path.
func (r *Reader) Read(path string) (recipes []Recipe, err error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	switch mode := fi.Mode(); {
	case mode.IsDir():
		recipes, err = r.readDir(path)
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

	err = yaml.Unmarshal(buff.Bytes(), &recipe)
	if err != nil {
		return
	}

	return
}

func (r *Reader) readDir(path string) (recipes []Recipe, err error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return
	}

	for _, entry := range entries {
		recipe, err := r.readFile(filepath.Join(path, entry.Name()))
		if err != nil {
			continue
		}

		recipes = append(recipes, recipe)
	}

	return
}
