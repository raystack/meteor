package file

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/models"
	assetsv1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
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
	data := make([]*assetsv1beta2.Asset, 0, len(batch))
	for _, record := range batch {
		data = append(data, record.Data())
	}

	if s.format == "ndjson" {
		return s.ndjsonOut(data)
	}

	return s.yamlOut(data)
}

func (s *Sink) Close() (err error) {
	return s.File.Close()
}

func (s *Sink) ndjsonOut(data []*assetsv1beta2.Asset) error {
	var result bytes.Buffer
	for _, asset := range data {
		jsonBytes, err := models.ToJSON(asset)
		if err != nil {
			return fmt.Errorf("error marshaling asset (%s): %w", asset.Urn, err)
		}

		result.Write(jsonBytes)
		result.WriteRune('\n')
	}

	if err := s.writeBytes(result.Bytes()); err != nil {
		return fmt.Errorf("error writing to file: %w", err)
	}

	return nil
}

func (s *Sink) yamlOut(data []*assetsv1beta2.Asset) error {
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
