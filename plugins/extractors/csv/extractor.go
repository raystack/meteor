package csv

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/odpf/meteor/utils"

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

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	e.logger.Info("extracting csv metadata...")

	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return extractor.InvalidConfigError{}
	}

	err = validateConfig(config)

	if err != nil {
		return err
	}
	files, err := getCSVFiles(config)

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

		stat, _ := csvFile.Stat()

		out <- createMetaTable(stat.Name(), file, columns)

	}

	return nil
}

// Validate configurations for csv extractor
func validateConfig(config Config) error {
	if config.FilePath == "" && config.DirectoryPath == "" {
		return extractor.InvalidConfigError{}
	}
	return nil
}

func getCSVFiles(config Config) (files []string, err error) {
	if config.DirectoryPath != "" {
		fileInfos, err := ioutil.ReadDir(config.DirectoryPath)

		if err != nil {
			return files, err
		}
		for _, fileInfo := range fileInfos {
			ext := filepath.Ext(fileInfo.Name())
			if ext == ".csv" {
				files = append(files, path.Join(config.DirectoryPath, fileInfo.Name()))
			}
		}
		return files, err
	}
	return []string{config.FilePath}, err
}

func createMetaTable(fileName string, filePath string, columns []*facets.Column) *meta.Table {
	return &meta.Table{
		Name:   fileName,
		Source: "csv",
		Schema: &facets.Columns{
			Columns: columns,
		},
		Custom: &facets.Custom{
			CustomProperties: map[string]string{"FilePath": filePath},
		},
	}
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

// Register the extractor to catalog
func init() {
	if err := extractor.Catalog.Register("csv", func() core.Extractor {
		return &Extractor{
			logger: plugins.Log,
		}
	}); err != nil {
		panic(err)
	}
}
