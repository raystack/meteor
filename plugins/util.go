package plugins

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"sort"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/goto/meteor/models"
	"github.com/mcuadros/go-defaults"
	"github.com/mitchellh/mapstructure"
)

var validate *validator.Validate

func init() {
	validate = validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		configName := strings.SplitN(fld.Tag.Get("mapstructure"), ",", 2)[0]

		if configName == "-" {
			return ""
		}
		return configName
	})
}

// BuildConfig builds a config struct from a map
func buildConfig(configMap map[string]interface{}, c interface{}) (err error) {
	defaults.SetDefaults(c)

	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(), mapstructure.StringToSliceHookFunc(","),
		),
		WeaklyTypedInput: true,
		Result:           c,
	})
	if err != nil {
		return fmt.Errorf("create new mapstructure decoder: %w", err)
	}
	if err = dec.Decode(configMap); err != nil {
		return fmt.Errorf("decode with mapstructure: %w", err)
	}
	if err = validate.Struct(c); err == nil {
		return nil
	}

	var validationErr validator.ValidationErrors
	if errors.As(err, &validationErr) {
		var configErrors []ConfigError
		for _, fieldErr := range validationErr {
			key := strings.TrimPrefix(fieldErr.Namespace(), "Config.")
			configErrors = append(configErrors, ConfigError{
				Key:     key,
				Message: fmt.Sprintf("validation for field '%s' failed on the '%s' tag", key, fieldErr.Tag()),
			})
		}
		return InvalidConfigError{
			Errors: configErrors,
		}
	}

	return err
}

func BigQueryTableFQNToURN(fqn string) (string, error) {
	projectID, datasetID, tableID, err := parseBQTableFQN(fqn)
	if err != nil {
		return "", fmt.Errorf("map URN: %w", err)
	}

	return BigQueryURN(projectID, datasetID, tableID), nil
}

func BigQueryURN(projectID, datasetID, tableID string) string {
	fqn := fmt.Sprintf("%s:%s.%s", projectID, datasetID, tableID)
	return models.NewURN("bigquery", projectID, "table", fqn)
}

func KafkaURN(bootstrapServers, topic string) string {
	return models.NewURN("kafka", KafkaServersToScope(bootstrapServers), "topic", topic)
}

func KafkaServersToScope(servers string) string {
	if strings.IndexRune(servers, ',') > 0 {
		// there are multiple bootstrap servers, just strip port, sort and join
		var hh []string
		for _, s := range strings.Split(servers, ",") {
			host, _, err := net.SplitHostPort(s)
			if err != nil {
				hh = append(hh, s)
				continue
			}

			hh = append(hh, host)
		}
		sort.Strings(hh)
		return strings.Join(hh, ",")
	}

	host, _, err := net.SplitHostPort(servers)
	if err != nil {
		return servers
	}

	return host
}

func CaraMLStoreURN(scope, project, featureTable string) string {
	return models.NewURN("caramlstore", scope, "feature_table", project+"."+featureTable)
}

// DrainBody drains and closes the response body to avoid the following
// gotcha:
// http://devs.cloudimmunity.com/gotchas-and-common-mistakes-in-go-golang/index.html#close_http_resp_body
func DrainBody(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func parseBQTableFQN(fqn string) (projectID, datasetID, tableID string, err error) {
	// fqn is the ID of the table in projectID:datasetID.tableID format.
	if !strings.ContainsRune(fqn, ':') || strings.IndexRune(fqn, '.') < strings.IndexRune(fqn, ':') {
		return "", "", "", fmt.Errorf(
			"unexpected BigQuery table FQN '%s', expected in format projectID:datasetID.tableID", fqn,
		)
	}
	ss := strings.FieldsFunc(fqn, func(r rune) bool {
		return r == ':' || r == '.'
	})
	return ss[0], ss[1], ss[2], nil
}
