package file

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/MakeNowJust/heredoc"
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
	"gopkg.in/yaml.v3"
)

//go:embed README.md
var summary string

type Config struct {
	Overwrite bool   `mapstructure:"overwrite" default:"true"`
	Path      string `mapstructure:"path" validate:"required"`
	Format    string `mapstructure:"format" validate:"required"`
}

var info = plugins.Info{
	Description: "Save output to a file",
	Summary:     summary,
	Tags:        []string{"file", "json", "yaml", "sink"},
	SampleConfig: heredoc.Doc(`
	path: ./output-filename.txt
	format: ndjson
	`),
}

type Sink struct {
	plugins.BasePlugin
	logger log.Logger
	config Config
	format string
	File   *os.File
}

func New(logger log.Logger) plugins.Syncer {
	s := &Sink{
		logger: logger,
	}
	s.BasePlugin = plugins.NewBasePlugin(info, &s.config)

	return s
}

func (s *Sink) Init(ctx context.Context, config plugins.Config) error {
	if err := s.BasePlugin.Init(ctx, config); err != nil {
		return err
	}

	if err := s.validateFilePath(s.config.Path); err != nil {
		return err
	}

	s.format = s.config.Format
	var (
		f   *os.File
		err error
	)
	if s.config.Overwrite {
		f, err = os.Create(s.config.Path)
	} else {
		f, err = os.OpenFile(s.config.Path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	}
	if err != nil {
		return err
	}

	s.File = f
	return nil
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	if s.format == "ndjson" {
		return s.ndjsonOut(batch)
	}

	return s.yamlOut(batch)
}

func (s *Sink) Close() (err error) {
	return s.File.Close()
}

func (s *Sink) ndjsonOut(batch []models.Record) error {
	var result bytes.Buffer
	for _, record := range batch {
		jsonBytes, err := models.RecordToJSON(record)
		if err != nil {
			return fmt.Errorf("error marshaling record (%s): %w", record.Entity().GetUrn(), err)
		}

		result.Write(jsonBytes)
		result.WriteRune('\n')
	}

	if err := s.writeBytes(result.Bytes()); err != nil {
		return fmt.Errorf("error writing to file: %w", err)
	}

	return nil
}

func (s *Sink) yamlOut(batch []models.Record) error {
	// Convert records to JSON-friendly maps for yaml serialization
	var data []map[string]interface{}
	for _, record := range batch {
		jsonBytes, err := models.RecordToJSON(record)
		if err != nil {
			return err
		}
		var m map[string]interface{}
		if err := json.Unmarshal(jsonBytes, &m); err != nil {
			return err
		}
		data = append(data, m)
	}

	ymlByte, err := yaml.Marshal(data)
	if err != nil {
		return err
	}

	return s.writeBytes(ymlByte)
}

func (s *Sink) writeBytes(b []byte) error {
	_, err := s.File.Write(b)
	return err
}

func (s *Sink) validateFilePath(path string) error {
	dirs := strings.Split(path, "/")
	filename := dirs[len(dirs)-1]
	format := strings.Split(filename, ".")
	if len(format) != 2 {
		return fmt.Errorf("invalid filename for \"%s\"", path)
	}
	return nil
}

func init() {
	if err := registry.Sinks.Register("file", func() plugins.Syncer {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
