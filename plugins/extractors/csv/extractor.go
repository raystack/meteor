package csv

import (
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/odpf/meteor/utils"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

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

func (e *Extractor) Extract(configMap map[string]interface{}) (results []meta.Table, err error) {
	e.logger.Info("extracting csv metadata...")
	var config Config

	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return results, extractor.InvalidConfigError{}
	}

	err = validateConfig(config)

	if err != nil {
		return results, err
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

		stat, err := csvFile.Stat()

		results = append(results, createMetaTable(stat.Name(), file, columns))

	}

	return results, err
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

func validateConfig(config Config) error {
	if config.FilePath == "" && config.DirectoryPath == "" {
		return extractor.InvalidConfigError{}
	}
	return nil
}

func createMetaTable(fileName string, filePath string, columns []*facets.Column) meta.Table {
	metaTable := meta.Table{
		Name:   fileName,
		Source: "csv",
		Schema: &facets.Columns{
			Columns: columns,
		},
		Custom: &facets.Custom{
			CustomProperties: map[string]string{"FilePath": filePath},
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
