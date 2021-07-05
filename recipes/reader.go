package recipes

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

var (
	recipeEnvVarPrefix = "METEOR_"
)

type Reader struct {
	data map[string]string
}

func NewReader() *Reader {
	reader := &Reader{}
	reader.populateData()

	return reader
}

func (r *Reader) Read(path string) (recipe Recipe, err error) {
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

func (r *Reader) ReadDir(path string) (recipes []Recipe, err error) {
	dirEntries, err := os.ReadDir(path)
	if err != nil {
		return
	}

	for _, dirEntry := range dirEntries {
		recipe, err := r.Read(filepath.Join(path, dirEntry.Name()))
		if err != nil {
			continue
		}

		recipes = append(recipes, recipe)
	}

	return
}

func (r *Reader) populateData() {
	data := make(map[string]string)
	for _, envvar := range os.Environ() {
		keyval := strings.SplitN(envvar, "=", 2) // "sampleKey=sample=Value" returns ["sampleKey", "sample=value"]
		key := keyval[0]
		val := keyval[1]

		key, ok := r.mapToMeteorKey(key)
		if !ok {
			continue
		}

		data[key] = val
	}

	r.data = data
}

func (r *Reader) mapToMeteorKey(rawKey string) (key string, ok bool) {
	// we are doing everything in lowercase for case insensitivity
	key = strings.ToLower(rawKey)
	meteorPrefix := strings.ToLower(recipeEnvVarPrefix)
	keyPrefixLen := len(meteorPrefix)

	isMeteorKeyFormat := len(key) > keyPrefixLen && key[:keyPrefixLen] == meteorPrefix
	if !isMeteorKeyFormat {
		return
	}
	key = key[keyPrefixLen:] // strips prefix - meteor_user_id becomes user_id
	ok = true

	return
}
