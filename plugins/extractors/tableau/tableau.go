package tableau

import (
	"context"
	_ "embed" // used to print the embedded assets

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

type Config struct {
	UserID    string `mapstructure:"user_id" validate:"required"`
	Password  string `mapstructure:"password" validate:"required"`
	Host      string `mapstructure:"host" validate:"required"`
	SessionID string `mapstructure:"session_token"`
}

var sampleConfig = `
host: localhost:1433
user_id: user@gmail.com
password: pass@1234`

type Extractor struct {
	logger log.Logger
	config Config
	// client *http.Client
}

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Info returns the brief information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Dashboard metadata and metrics from Tableau sever.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config map[string]interface{}) (err error) {
	// Build and validate config received from recipe
	if err := utils.BuildConfig(config, &e.config); err != nil {
		return plugins.InvalidConfigError{}
	}

	// Establish connection

	// if err != nil {
	// return errors.Wrap(err, "failed to create connection")
	// }

	return
}

// Extract collects the metadata from the source. The metadata is collected through the out channel
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	// dashboards, err := e.getDashboardsList()
	// if err != nil {
	// return errors.Wrap(err, "failed to fetch dashboard list")
	// }
	// for _, dashboard := range dashboards {
	// data, err := e.buildDashboard(strconv.Itoa(dashboard.ID), dashboard.Name)
	// if err != nil {
	// return errors.Wrap(err, "failed to fetch dashboard data")
	// }
	// emit(models.NewRecord(data))
	// }
	return nil
}

// func (e *Extractor) getSessionID() (sessionID string, err error) {
// if e.config.SessionID != "" {
// return e.config.SessionID, nil
// }
//
// payload := map[string]interface{}{
// "username": e.config.UserID,
// "password": e.config.Password,
// }
// type responseID struct {
// ID string `json:"id"`
// }
// var data responseID
// err = e.makeRequest("POST", e.config.Host+"/api/session", payload, &data)
// if err != nil {
// return
// }
// return data.ID, nil
// }

// helper function to avoid rewriting a request
// func (e *Extractor) makeRequest(method, url string, payload interface{}, data interface{}) (err error) {
// jsonifyPayload, err := json.Marshal(payload)
// if err != nil {
// return errors.Wrap(err, "failed to encode the payload JSON")
// }
// body := bytes.NewBuffer(jsonifyPayload)
// req, err := http.NewRequest(method, url, body)
// if err != nil {
// return errors.Wrap(err, "failed to create request")
// }
// if body != nil {
// req.Header.Set("Content-Type", "application/json")
// }
// if e.config.SessionID != "" {
// req.Header.Set("X-Metabase-Session", e.config.SessionID)
// }
// res, err := e.client.Do(req)
// if err != nil {
// return errors.Wrap(err, "failed to generate response")
// }
// b, err := ioutil.ReadAll(res.Body)
// if err != nil {
// return errors.Wrap(err, "failed to read response body")
// }
// if err = json.Unmarshal(b, &data); err != nil {
// return errors.Wrapf(err, "failed to parse: %s", string(b))
// }
// return
// }

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("tableau", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
