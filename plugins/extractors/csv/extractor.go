package csv

import (
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/odpf/meteor/utils"
	"io"
	"os"

	"encoding/csv"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
)

type Config struct {
	DirectoryPath string `mapstructure:"directory"`
	FilePath      string `mapstructure:"file"`
}

type Extractor struct {
	logger plugins.Logger
}

func New(logger plugins.Logger) extractor.TableExtractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []meta.Table, err error) {
	e.logger.Info("extracting csv metadata...")
	var config Config

	err = utils.BuildConfig(configMap, &config)
	var results []meta.Table
	if err != nil {
		return results, extractor.InvalidConfigError{}
	}

	err = validateConfig(config)

	if err != nil {
		return results, err
	}
	files := getFiles(config)
	for _, file := range files {
		csvFile, err := os.Open(file)
		if err != nil {
			e.logger.Error("unable to open the csv file ", file)
			continue
		}
		defer csvFile.Close()
		content, err := readCSVFile(csvFile)

		if err != nil {
			e.logger.Error("Not able to read csv file")
			continue
		}
		columns, err := getColumns(content)
		if err != nil {
			e.logger.Error("Not able convert to columns")
			continue
		}
		results = append(result, createMetaTable(file, columns))
	}

	// todo : how to handle files which are not csv files
	// todo : how to handle directory
	return results, err
}

func getFiles(config Config) []string {
	return []string{config.FilePath}
}

func validateConfig(config Config) error {
	if config.FilePath == "" && config.DirectoryPath == "" {
		return extractor.InvalidConfigError{}
	}
	return nil
}

func createMetaTable(fileName string, columns []*facets.Column) meta.Table {
	metaTable := meta.Table{
		Name:   fileName,
		Source: "csv",
		Schema: &facets.Columns{
			Columns: columns,
		},
	}
	return metaTable
}

func readCSVFile(r io.Reader) (columns []string, err error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	return reader.Read()
}

func getColumns(csvColumns []string) (result []*facets.Column, err error) {
	for _, singleColumn := range csvColumns {
		result = append(result, &facets.Column{
			Name: singleColumn,
		})
	}
	return result, nil
}
