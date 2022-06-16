package file

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"gopkg.in/yaml.v3"
)

//go:embed README.md
var summary string

type Config struct {
	Path   string `mapstructure:"path" validate:"required"`
	Format string `mapstructure:"format" validate:"required"`
}

var sampleConfig = `
path: ./dir/some-dir/postgres_food_app_data.json
format: json
`

var data []models.Metadata

type Sink struct {
	logger log.Logger
	config Config
	format string
}

func New() plugins.Syncer {
	return new(Sink)
}

func (s *Sink) Info() plugins.Info {
	return plugins.Info{
		Description:  "save output to a file",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"file", "json", "yaml", "sink"},
	}
}

func (s *Sink) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (s *Sink) Init(ctx context.Context, config map[string]interface{}) (err error) {
	if err := utils.BuildConfig(config, &s.config); err != nil {
		return plugins.InvalidConfigError{Type: "sink", PluginName: "file"}
	}

	if err := s.validateFilePath(s.config.Path); err != nil {
		return err
	}
	s.format = s.config.Format
	return
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	for _, record := range batch {
		data = append(data, record.Data())
	}
	return nil
}

func (s *Sink) Close() (err error) {
	if s.format == "json" {
		err := s.jsonOut(data)
		if err != nil {
			return err
		}
		return nil
	}
	err = s.yamlOut(data)
	if err != nil {
		return err
	}
	return nil
}

func (s *Sink) jsonOut(data []models.Metadata) error {
	jsnBy, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(s.config.Path, jsnBy, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (s *Sink) yamlOut(data []models.Metadata) error {
	ymlByte, err := yaml.Marshal(data)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(s.config.Path, ymlByte, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (s *Sink) validateFilePath(path string) error {
	dirs := strings.Split(path, "/")
	filename := dirs[len(dirs)-1]
	format := strings.Split(filename, ".")
	if len(format) != 2 {
		return fmt.Errorf("invalid filename")
	}
	return nil
}

func init() {
	if err := registry.Sinks.Register("file", func() plugins.Syncer {
		return &Sink{
			logger: plugins.GetLog(),
		}
	}); err != nil {
		panic(err)
	}
}
