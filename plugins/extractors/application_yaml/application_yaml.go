package applicationyaml

import (
	"bytes"
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"os"
	"reflect"
	"strings"
	"text/template"

	"github.com/MakeNowJust/heredoc"
	"github.com/go-playground/validator/v10"
	"github.com/goto/meteor/models"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	"gopkg.in/yaml.v3"
)

var validate = validator.New()

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("application_yaml", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}

	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("yaml"), ",", 2)[0]
		if name == "-" {
			return ""
		}
		return name
	})
}

//go:embed README.md
var summary string

// Config holds the set of configuration for the CaraML Store extractor
type Config struct {
	File      string `mapstructure:"file" validate:"required,file"`
	EnvPrefix string `mapstructure:"env_prefix" default:"CI" validate:"required"`
}

var sampleConfig = heredoc.Doc(`
	file: "./path/to/application.meteor.yaml"
	env_prefix: CI
`)

var info = plugins.Info{
	Description:  "Application metadata from YAML file",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"application", "file"},
}

// Extractor is the extractor instance for application YAML file.
type Extractor struct {
	plugins.BaseExtractor

	logger log.Logger
	config Config
}

func New(logger log.Logger) *Extractor {
	e := Extractor{logger: logger}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)
	return &e
}

func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) error {
	tmpl, err := template.ParseFiles(e.config.File)
	if err != nil {
		return fmt.Errorf("application_yaml extract: parse file: %w", err)
	}

	tmpl.Option("missingkey=error")

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, loadEnvVars(e.config.EnvPrefix)); err != nil {
		return fmt.Errorf("application_yaml extract: inject env vars: %w", err)
	}

	var svc Application
	d := yaml.NewDecoder(&buf)
	d.KnownFields(true)
	if err := d.Decode(&svc); err != nil {
		return fmt.Errorf("application_yaml extract: load application: %w", err)
	}

	if err := validate.Struct(svc); err != nil {
		return fmt.Errorf("application_yaml extract: validate: %w", err)
	}

	// Build an asset from the Application with lineage and emit
	asset, err := buildAsset(e.UrnScope, svc)
	if err != nil {
		return fmt.Errorf("application_yaml extract: build asset: %w", err)
	}

	emit(models.NewRecord(asset))

	return nil
}

func loadEnvVars(prefix string) map[string]string {
	res := map[string]string{}
	for _, envVar := range os.Environ() {
		kv := strings.SplitN(envVar, "=", 2)
		if k, ok := varName(kv[0], prefix); ok {
			res[k] = kv[1]
		}
	}

	return res
}

func varName(s, prefix string) (string, bool) {
	if len(s) <= len(prefix) || !strings.EqualFold(s[:len(prefix)], prefix) {
		return "", false
	}

	r := strings.ToLower(s[len(prefix):])
	if strings.HasPrefix(r, "_") {
		return r[1:], true
	}
	return r, true
}
