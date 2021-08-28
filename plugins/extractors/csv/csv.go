package csv

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/proto/odpf/assets/facets"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/pkg/errors"

	"encoding/csv"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/salt/log"
)

type Config struct {
	Path string `mapstructure:"path" validate:"required"`
}

type Extractor struct {
	logger log.Logger
}

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	// build config
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	// build file paths to read from
	filePaths, err := e.buildFilePaths(config.Path)
	if err != nil {
		return
	}

	return e.extract(filePaths, out)
}

func (e *Extractor) extract(filePaths []string, out chan<- interface{}) (err error) {
	for _, filePath := range filePaths {
		table, err := e.buildTable(filePath)
		if err != nil {
			return fmt.Errorf("error building metadata for \"%s\": %s", filePath, err)
		}

		out <- table
	}

	return
}

func (e *Extractor) buildTable(filePath string) (table assets.Table, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		err = errors.New("unable to open the csv file")
		return
	}
	defer file.Close()
	content, err := e.readCSVFile(file)
	if err != nil {
		err = errors.New("unable to read csv file content")
		return
	}
	stat, err := file.Stat()
	if err != nil {
		err = errors.New("unable to read csv file stat")
		return
	}

	fileName := stat.Name()
	table = assets.Table{
		Resource: &common.Resource{
			Urn:     fileName,
			Name:    fileName,
			Service: "csv",
		},
		Schema: &facets.Columns{
			Columns: e.buildColumns(content),
		},
	}
	return
}

func (e *Extractor) readCSVFile(r io.Reader) (columns []string, err error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	return reader.Read()
}

func (e *Extractor) buildColumns(csvColumns []string) (result []*facets.Column) {
	for _, singleColumn := range csvColumns {
		result = append(result, &facets.Column{
			Name: singleColumn,
		})
	}
	return result
}

func (e *Extractor) buildFilePaths(filePath string) (files []string, err error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return
	}

	if fileInfo.IsDir() {
		fileInfos, err := ioutil.ReadDir(filePath)
		if err != nil {
			return files, err
		}
		for _, fileInfo := range fileInfos {
			ext := filepath.Ext(fileInfo.Name())
			if ext == ".csv" {
				files = append(files, path.Join(filePath, fileInfo.Name()))
			}
		}
		return files, err
	}

	return []string{filePath}, err
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("csv", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
