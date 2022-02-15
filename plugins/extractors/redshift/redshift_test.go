package redshift_test

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/odpf/meteor/test/utils"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	"log"
	"os"
	"testing"
)

const (
	testDB    = "test_db"
	user      = "test_user"
	pass      = "pass"
	port      = "5439"
	root      = "root"
	defaultDB = "postgres"
)

var (
	host = "127.0.0.1:" + port
	db   *sql.DB
)

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository: "hearthsim/pgredshift",
		Tag:        "latest",
		Env: []string{
			"POSTGRES_USER=" + root, "POSTGRES_PASSWORD=" + pass, "POSTGRES_DB=" + defaultDB,
		},
		ExposedPorts: []string{port, "5432"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5439": {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}

	// Exponential backoff-retry for container to accept connections
	retryFn := func(r *dockertest.Resource) (err error) {
		db, err = sql.Open("postgres", fmt.Sprintf("postgres://root:%s@%s/%s?sslmode=disable", pass, host, defaultDB))
		if err != nil {
			return err
		}
		if err := r.Expire(120); err != nil {
			log.Fatal("closed")
		}
		return db.Ping()
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}
	//if err := setup(); err != nil {
	// log.Fatal(err)
	//}

	// Run tests
	code := m.Run()

	// Clean tests
	if err = db.Close(); err != nil {
		log.Fatal(err)
	}
	if err = purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}
