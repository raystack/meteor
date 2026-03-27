//go:build plugins
// +build plugins

package gcs_test

import (
	"context"
	"testing"

	"github.com/raystack/meteor/models"
	v1beta2 "github.com/raystack/meteor/models/raystack/assets/v1beta2"
	"github.com/raystack/meteor/plugins"
	g "github.com/raystack/meteor/plugins/sinks/gcs"
	testUtils "github.com/raystack/meteor/test/utils"
	"github.com/raystack/meteor/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/anypb"
)

var validConfig = map[string]interface{}{
	"project_id":             "google-project-id",
	"url":                    "gcs://bucket_name/target_folder",
	"object_prefix":          "github-users",
	"service_account_base64": "base 64 encoded key",
}

func TestInit(t *testing.T) {
	t.Run("should return error if config is invalid", func(t *testing.T) {
		gcsSink := g.New(testUtils.Logger)
		actualError := gcsSink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
			"project_id": "",
		}})
		assert.ErrorAs(t, actualError, &plugins.InvalidConfigError{})
	})

	t.Run("should retun error if service account json and service account base64 missing", func(t *testing.T) {
		gcsSink := g.New(testUtils.Logger)
		actualError := gcsSink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
			"project_id": "google-project-id",
			"url":        "gcs://bucket_name/target_folder",
		}})
		assert.ErrorContains(t, actualError, "credentials are not specified, failed to create client")
	})

	t.Run("should retun error if unable to decode base64 service account key", func(t *testing.T) {
		gcsSink := g.New(testUtils.Logger)
		actualError := gcsSink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
			"project_id":             "google-project-id",
			"url":                    "gcs://bucket_name/target_folder",
			"service_account_base64": "----", // invalid
		}})
		assert.ErrorContains(t, actualError, "decode base64 service account")
	})
}

func TestSink(t *testing.T) {
	t.Run("should write data in bucket and return nil error on success", func(t *testing.T) {
		u := &v1beta2.User{
			FullName: "John Doe",
			Email:    "john.doe@raystack.com",
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"org_unit_path": "/",
				"aliases":       "doe.john@raystack.com,johndoe@raystack.com",
			}),
		}
		user, _ := anypb.New(u)
		data := &v1beta2.Asset{
			Data: user,
		}
		jsonBytes, _ := models.ToJSON(data)

		ctx := context.TODO()
		writer := new(mockWriter)
		writer.On("WriteData", jsonBytes).Return(nil)
		writer.On("WriteData", []byte("\n")).Return(nil)

		gcsSink := g.New(testUtils.Logger)

		err := gcsSink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
			"project_id":           "google-project-id",
			"url":                  "gcs://bucket_name/target_folder",
			"service_account_json": `{"type": "service_account", "project_id": "google-project-id", "private_key_id": "key-id", "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA2a2rwplBQLF29amygykEMmYz0+Kcj3bKBp29Di0DBBF0rRi/\nP99VnCcGd6sEONOEkHRCxUSm9VkOcMPmLR99yV3Opm5VFN5Eo+bRAdRtS1/5p/O\nyKD9S0N5vB0PiNdwgkvVHuFFkvM3UPvqz/IHEL1MnxNjcmHnJi2K/Z6/6hKF0J/A\njAVPunUiR7VjSWTHNsB1BtYOBSGL1Uqe8a0IZjJjJRhIAo8HqHSzMGJNR6SNj3A\nlkW/oy8L3w3GJpbBbBPdGydE0qlJEJkWBNuvp7oK8JMBzFXlse6MkxGbG5eM1mra\n/VG+qJPpGrKxzh7NZXa8EEulKGMaFIwizkVYLQIDAQABAoIBAF5p8DFkVpOeWd23\nK8MhqUR6NJGG5n6A56GaHjwkzgFMsl1yVHImf0quwZbpMhJgW6L4I4qLcDpbfj2f\na2DLHS1FuoviYXIBCHj4A5L3v0kQzBOvPBVSqz5fBr0+GY+afcREdvVHCGw+6doh\nPa/gdQTNsn9bOGJwHIhM4qJK8dTGl86r1JFsTerVxJzT+4Y+E1wDiQxBkDHhkb7W\nlQE7DQEC11j73pDtU1GCMFqyyCCDGpcxhfOFc8YCfjXMYfszBsBBfNYpRP3GR+lz\nIqUa0nZ8VVxFVRCjbZ8S0K2HVMqUm/4M8H3Y8TJnaiJLJT86v4Tj9F7ehmcOuGjH\nF1aDPQECgYEA7Z/29ILTEALEqkz0h7lS5sR7Y7K7FDf2I2kOZj6HN/mScWi7LgMd\nmA5IKd2Y0jjBl0c3zPZH7gTI6tGJaMbZ+gPK7Vg97VL9o5oj+Cjz2ONWQ0fxVVL+\nsxHLBhfJUMFHBU0qWJPczmC4dvT5+xXmjDPdIQ5krM7sYiqF8vG00EECgYEA6k0N\nKFFO0bQhzPSB+xRiId4i3fXMHMTzC3E3Zzra7E3Y3HRaCxYTEd7e7LIRL/D1GKLu\nm8vc8yzFPd2zfXmG4eNrJJakPvh8a1PFbDcd7ZYLbiJo7E0J8GqmmlC3C80i+IV/\nnUl+Z9rF7b1f9k1Oia3e0fM+BeFJQCRz0J1vlQ0CgYBJqiJMBkFaZKHqbg/dV7vC\nFM0m/WRxKQ7O3G0yiK3Y3FAOtUxF1DIBSRsfBOCjKqJz8N4TTVA5yJNNRq2b/Gjh\nf0dnJUk4b2R3U7hSWU8pEOvVmQDAn7RxYS29t5eK02HU7MZroJPIbPxgJ8K+aNqZ\nHRJUlHbjn+BkOvOAdWk9AQKBgCMRLjOvDPWPHPeKBCuD8M3dZR6FuJN5CUmq+3rP\nQAZnVJbLP+5DmXJUHPDWIjQmGwz/a1Rt+1CCBP+bJMTG6ek4fjFhHklnT5Eo6VAJ\nJBHIeRKGlb03rc0jwH3e3V0rNtNlkJbHW5Z2JVVJeOcwA3JmRJ6fRKJH2NJE+L/j\nqDGNAoGBAJLV5/eJB0E2/jvsBGHr7SCUO8CgoM98Z0qZpSgnYi0LaN/oX2vTwcnD\neX0t8D1J7Y8N0FzXH+02La3+SZFhYBMi7D2j8DRNDFrg3qJbBl1aJ/R1Y3LJjwBZ\nB+sMx39JTb7C0w8V1SuXhH9F56EI3swv67Bpy/1a6KHyLFKMj3bD\n-----END RSA PRIVATE KEY-----\n", "client_email": "test@google-project-id.iam.gserviceaccount.com", "client_id": "123456789", "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token"}`,
			"object_prefix":        "github-users",
		}})
		if err != nil {
			t.Fatal(err)
		}

		defer gcsSink.Close()
		err = gcsSink.Sink(ctx, []models.Record{models.NewRecord(data)})

		assert.NoError(t, err)
	})
}

type mockWriter struct {
	mock.Mock
}

func (m *mockWriter) WriteData(jsonBytes []byte) error {
	args := m.Called(jsonBytes)

	return args.Error(0)
}

func (m *mockWriter) Close() error {
	args := m.Called()

	return args.Error(0)
}
