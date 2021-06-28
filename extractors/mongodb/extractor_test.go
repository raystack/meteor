package mongodb_test

import (
	"testing"

	"github.com/odpf/meteor/extractors/mongodb"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestExtract(t *testing.T) {
	t.Run("should return error if no user_id in config", func(t *testing.T) {
		extractor := new(mongodb.Extractor)
		_, err := extractor.Extract(map[string]interface{}{})

		assert.NotNil(t, err)
	})

	t.Run("should return error if no password in config", func(t *testing.T) {
		extractor := new(mongodb.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"user_id": "Gaurav_Ubuntu",
		})

		assert.NotNil(t, err)
	})

	t.Run("should return error while testing without mongo running", func(t *testing.T) {
		extractor := new(mongodb.Extractor)
		uri := "mongodb://user:abcd@localhost:27017"
		clientOptions := options.Client().ApplyURI(uri)
		err := mongodb.MockDataGenerator(clientOptions)
		_, err = extractor.Extract(map[string]interface{}{
			"user_id":  "user",
			"password": "abcd",
		})
		assert.NotNil(t, err)
	})
}
