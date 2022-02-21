//go:build integration
// +build integration

package presto_test

import (
	"database/sql"
	"fmt"
	"github.com/odpf/meteor/test/utils"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	_ "github.com/prestodb/presto-go-client/presto"
	"log"
	"os"
	"testing"
)

const (
	testDB = "test_db"
	user   = "test_user"
	pass   = "pass"
	port   = "8080"
)

var (
	host = "127.0.0.1:" + port
	db   *sql.DB
)

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository:   "starburstdata/presto",
		Tag:          "350-e.18",
		Env:          []string{},
		ExposedPorts: []string{"8080"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8080": {
				{HostIP: "0.0.0.0", HostPort: "8080"},
			},
		},
	}

	// Exponential backoff-retry for container to accept connections
	retryFn := func(r *dockertest.Resource) (err error) {
		//dsn := "http://user@localhost:8080?catalog=default&schema=test"
		//dsn - http[s]://user[:pass]@host[:port][?parameters]
		dsn := fmt.Sprintf("https://%s:%s@%s?catalog=default&schema=test", user, pass, host)
		db, err = sql.Open("presto", dsn)
		if err != nil {
			return err
		}
		return db.Ping()
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}
	//if err := setup(); err != nil {
	//	log.Fatal(err)
	//}

	// Run tests
	code := m.Run()

	// Clean tests
	db.Close()
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}
