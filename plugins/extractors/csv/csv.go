package csv

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	"github.com/odpf/meteor/registry"
	"github.com/pkg/errors"

	"encoding/csv"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

// Config holds the path configuration for the csv extractor
type Config struct {
	Path string `mapstructure:"path" validate:"required"`
}

var sampleConfig = `
path: ./path-to-a-file-or-a-directory`

var info = plugins.Info{
	Description:  "Comma separated file",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"file", "extractor"},
}

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	plugins.BaseExtractor
	config    Config
	logger    log.Logger
	filePaths []string
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// build file paths to read from
	e.filePaths, err = e.buildFilePaths(e.config.Path)
	if err != nil {
		return
	}

	return
}

// Extract checks if the extractor is configured and
// returns the extracted data
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	for _, filePath := range e.filePaths {
		table, err := e.buildTable(filePath)
		if err != nil {
			return fmt.Errorf("error building metadata for \"%s\": %s", filePath, err)
		}

		emit(models.NewRecord(table))
	}

	return
}

func (e *Extractor) buildTable(filePath string) (table *v1beta2.Asset, err error) {
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
	table = &assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:     models.NewURN("csv", e.UrnScope, "file", fileName),
			Name:    fileName,
			Service: "csv",
			Type:    "table",
		},
		Schema: &facetsv1beta1.Columns{
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

func (e *Extractor) buildColumns(csvColumns []string) (result []*facetsv1beta1.Column) {
	for _, singleColumn := range csvColumns {
		result = append(result, &facetsv1beta1.Column{
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
