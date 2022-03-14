package extractorutils

import (
	"database/sql"
	"log"

	"github.com/odpf/meteor/plugins"
)

// Config holds the connection URL for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
}

// Extractor manages the extraction of data from MySQL
type Extractor struct {
	excludedDbs map[string]bool
	logger      log.Logger
	config      Config
	db          *sql.DB
	emit        plugins.Emit
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}
