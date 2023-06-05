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

func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// build file paths to read from
	var err error
	e.filePaths, err = e.buildFilePaths(e.config.Path)
	if err != nil {
		return err
	}

	return nil
}

// Extract checks if the extractor is configured and
// returns the extracted data
func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) error {
	for _, filePath := range e.filePaths {
		table, err := e.buildTable(filePath)
		if err != nil {
			return fmt.Errorf("build metadata for %q: %w", filePath, err)
		}

		emit(models.NewRecord(table))
	}

	return nil
}

func (e *Extractor) buildTable(filePath string) (*v1beta2.Asset, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open the csv file: %w", err)
	}
	defer file.Close()

	content, err := e.readCSVFile(file)
	if err != nil {
		return nil, fmt.Errorf("read csv file content: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("read csv file stat: %w", err)
	}

	table, err := anypb.New(&v1beta2.Table{
		Columns:    e.buildColumns(content),
		Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
	})
	if err != nil {
		return nil, fmt.Errorf("create Any struct for test: %w", err)
	}
	fileName := stat.Name()
	return &v1beta2.Asset{
		Urn:     models.NewURN("csv", e.UrnScope, "file", fileName),
		Name:    fileName,
		Service: "csv",
		Type:    "table",
		Data:    table,
	}, nil
}

func (*Extractor) readCSVFile(r io.Reader) ([]string, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	return reader.Read()
}

func (*Extractor) buildColumns(csvColumns []string) []*v1beta2.Column {
	var result []*v1beta2.Column
	for _, singleColumn := range csvColumns {
		result = append(result, &v1beta2.Column{
			Name: singleColumn,
		})
	}
	return result
}

func (*Extractor) buildFilePaths(filePath string) ([]string, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}

	if !fileInfo.IsDir() {
		return []string{filePath}, nil
	}

	fileInfos, err := os.ReadDir(filePath)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, f := range fileInfos {
		ext := filepath.Ext(f.Name())
		if ext == ".csv" {
			files = append(files, path.Join(filePath, f.Name()))
		}
	}
	return files, nil
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("csv", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
