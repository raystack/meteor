package csv

import (
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/logger"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strings"
	"testing"
)

var log = logger.NewWithWriter("info", ioutil.Discard)

func TestExtract(t *testing.T) {
	t.Run("should return error if fileName and directory both are empty", func(t *testing.T) {
		extr := New(log)
		_, err := extr.Extract(map[string]interface{}{
		})
		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

}

func TestGetFiles(t *testing.T) {
	t.Run("should return files from config with fileName", func(t *testing.T) {
		c := Config{FilePath: "test.csv"}
		files := getFiles(c)
		expectedResult := []string{"test.csv"}

		assert.Equal(t, expectedResult, files)
	})
}

func TestValidateConfig(t *testing.T) {
	t.Run("should return error if fileName and directory both are empty", func(t *testing.T) {
		c := Config{}
		err := validateConfig(c)
		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return nil if config is validated correctly", func(t *testing.T) {
		c := Config{FilePath: "test.csv"}
		err := validateConfig(c)
		assert.Equal(t, nil, err)
	})
}

func TestCreateMetaTable(t *testing.T) {
	t.Run("should build table from columns", func(t *testing.T) {
		var columns []*facets.Column

		table := createMetaTable("test.csv", columns)
		assert.Equal(t, "csv", table.Source)
		assert.Equal(t, "test.csv", table.Name)
		assert.Equal(t, columns, table.Schema.Columns)
	})
}

func TestReadCSVFile(t *testing.T) {
	t.Run("should return columns from a csv file", func(t *testing.T) {
		reader := strings.NewReader("a,b,c")
		content, err := readCSVFile(reader)
		assert.Equal(t, nil, err)
		assert.Equal(t, []string{"a", "b", "c"}, content)
	})
	t.Run("should ignore the white spaces", func(t *testing.T) {
		reader := strings.NewReader("a, b, c")
		content, err := readCSVFile(reader)
		assert.Equal(t, nil, err)
		assert.Equal(t, []string{"a", "b", "c"}, content)
	})
}

func TestGetColumns(t *testing.T) {
	t.Run("should convert csv columns to column facets", func(t *testing.T) {
		var columns []*facets.Column
		result, err := getColumns([]string{})
		assert.Equal(t, nil, err)
		assert.Equal(t, columns, result)
	})
}
