package csv

import (
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed README.md
var summary string

// Config holds the path configuration for the csv extractor
type Config struct {
	Path string `mapstructure:"path" validate:"required"`
}

var sampleConfig = `path: ./path-to-a-file-or-a-directory`

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

func (e *Extractor) buildTable(filePath string) (asset *v1beta2.Asset, err error) {
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

	table, err := anypb.New(&v1beta2.Table{
		Columns:    e.buildColumns(content),
		Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
	})
	if err != nil {
		err = fmt.Errorf("error creating Any struct for test: %w", err)
		return
	}
	fileName := stat.Name()
	asset = &v1beta2.Asset{
		Urn:     models.NewURN("csv", e.UrnScope, "file", fileName),
		Name:    fileName,
		Service: "csv",
		Type:    "table",
		Data:    table,
	}
	return
}

func (e *Extractor) readCSVFile(r io.Reader) (columns []string, err error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	return reader.Read()
}

func (e *Extractor) buildColumns(csvColumns []string) (result []*v1beta2.Column) {
	for _, singleColumn := range csvColumns {
		result = append(result, &v1beta2.Column{
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
		fileInfos, err := os.ReadDir(filePath)
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
