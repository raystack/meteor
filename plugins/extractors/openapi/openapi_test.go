package openapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/raystack/meteor/plugins"
	extractor "github.com/raystack/meteor/plugins/extractors/openapi"
	"github.com/raystack/meteor/test/mocks"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const urnScope = "test-api"

func TestInit(t *testing.T) {
	t.Run("should return error when source is missing", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope:  urnScope,
			RawConfig: map[string]any{},
		})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return error for invalid format", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"source": "./test.yaml",
				"format": "invalid",
			},
		})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should succeed with valid config", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"source": "./test.yaml",
			},
		})
		assert.NoError(t, err)
	})
}

func TestExtractOpenAPIv3(t *testing.T) {
	spec := `
openapi: "3.0.3"
info:
  title: Petstore
  description: A sample API for pets
  version: "1.0.0"
servers:
  - url: https://api.example.com/v1
paths:
  /pets:
    get:
      summary: List all pets
      operationId: listPets
    post:
      summary: Create a pet
      operationId: createPet
  /pets/{petId}:
    get:
      summary: Info for a specific pet
      operationId: showPetById
components:
  schemas:
    Pet:
      type: object
    Error:
      type: object
`
	dir := t.TempDir()
	specPath := filepath.Join(dir, "petstore.yaml")
	require.NoError(t, os.WriteFile(specPath, []byte(spec), 0o644))

	extr := initExtractor(t, map[string]any{
		"source": specPath,
	})

	emitter := mocks.NewEmitter()
	err := extr.Extract(context.Background(), emitter.Push)
	require.NoError(t, err)

	records := emitter.Get()
	require.Len(t, records, 1)

	entity := records[0].Entity()
	assert.Equal(t, "Petstore", entity.GetName())
	assert.Equal(t, "api", entity.GetType())
	assert.Equal(t, "openapi", entity.GetSource())
	assert.Equal(t, "A sample API for pets", entity.GetDescription())
	assert.Contains(t, entity.GetUrn(), "urn:openapi:test-api:api:petstore")

	props := entity.GetProperties().AsMap()
	assert.Equal(t, "1.0.0", props["version"])
	assert.Equal(t, "openapi_v3", props["format"])

	endpoints, ok := props["endpoints"].([]any)
	require.True(t, ok)
	assert.Len(t, endpoints, 3)

	schemas, ok := props["schemas"].(float64)
	require.True(t, ok)
	assert.Equal(t, float64(2), schemas)

	servers, ok := props["servers"].([]any)
	require.True(t, ok)
	assert.Len(t, servers, 1)
	assert.Equal(t, "https://api.example.com/v1", servers[0])
}

func TestExtractOpenAPIv2(t *testing.T) {
	spec := `{
  "swagger": "2.0",
  "info": {
    "title": "Legacy API",
    "version": "0.1.0"
  },
  "host": "api.legacy.com",
  "basePath": "/v1",
  "schemes": ["https"],
  "paths": {
    "/users": {
      "get": {
        "summary": "List users",
        "operationId": "listUsers"
      }
    }
  },
  "definitions": {
    "User": {},
    "Error": {},
    "Status": {}
  }
}`
	dir := t.TempDir()
	specPath := filepath.Join(dir, "legacy.json")
	require.NoError(t, os.WriteFile(specPath, []byte(spec), 0o644))

	extr := initExtractor(t, map[string]any{
		"source": specPath,
	})

	emitter := mocks.NewEmitter()
	err := extr.Extract(context.Background(), emitter.Push)
	require.NoError(t, err)

	records := emitter.Get()
	require.Len(t, records, 1)

	entity := records[0].Entity()
	assert.Equal(t, "Legacy API", entity.GetName())

	props := entity.GetProperties().AsMap()
	assert.Equal(t, "openapi_v2", props["format"])
	assert.Equal(t, "0.1.0", props["version"])
	assert.Equal(t, float64(3), props["schemas"])

	servers, ok := props["servers"].([]any)
	require.True(t, ok)
	require.Len(t, servers, 1)
	assert.Equal(t, "https://api.legacy.com/v1", servers[0])
}

func TestExtractProtobuf(t *testing.T) {
	proto := `
syntax = "proto3";

package mypackage;

message GetUserRequest {
  string id = 1;
}

message GetUserResponse {
  string id = 1;
  string name = 2;
}

message ListUsersRequest {}
message ListUsersResponse {}

service UserService {
  rpc GetUser(GetUserRequest) returns (GetUserResponse);
  rpc ListUsers(ListUsersRequest) returns (ListUsersResponse);
}

service AdminService {
  rpc DeleteUser(GetUserRequest) returns (GetUserResponse);
}
`
	dir := t.TempDir()
	protoPath := filepath.Join(dir, "user.proto")
	require.NoError(t, os.WriteFile(protoPath, []byte(proto), 0o644))

	extr := initExtractor(t, map[string]any{
		"source": protoPath,
	})

	emitter := mocks.NewEmitter()
	err := extr.Extract(context.Background(), emitter.Push)
	require.NoError(t, err)

	records := emitter.Get()
	require.Len(t, records, 1)

	entity := records[0].Entity()
	assert.Equal(t, "mypackage", entity.GetName())
	assert.Equal(t, "api", entity.GetType())

	props := entity.GetProperties().AsMap()
	assert.Equal(t, "proto3", props["version"])
	assert.Equal(t, "protobuf", props["format"])
	assert.Equal(t, "mypackage", props["package"])
	assert.Equal(t, float64(4), props["messages"])

	services, ok := props["services"].([]any)
	require.True(t, ok)
	assert.Len(t, services, 2)

	svc0, ok := services[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "UserService", svc0["name"])

	methods, ok := svc0["methods"].([]any)
	require.True(t, ok)
	assert.Len(t, methods, 2)
}

func TestExtractGlob(t *testing.T) {
	dir := t.TempDir()

	proto1 := `syntax = "proto3"; package svc1; service Svc1 { rpc Do(Req) returns (Resp); } message Req {} message Resp {}`
	proto2 := `syntax = "proto3"; package svc2; service Svc2 { rpc Run(Input) returns (Output); } message Input {} message Output {}`

	require.NoError(t, os.WriteFile(filepath.Join(dir, "svc1.proto"), []byte(proto1), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "svc2.proto"), []byte(proto2), 0o644))

	extr := initExtractor(t, map[string]any{
		"source": filepath.Join(dir, "*.proto"),
	})

	emitter := mocks.NewEmitter()
	err := extr.Extract(context.Background(), emitter.Push)
	require.NoError(t, err)

	records := emitter.Get()
	assert.Len(t, records, 2)
}

func TestExtractFromURL(t *testing.T) {
	spec := `{"openapi": "3.0.0", "info": {"title": "Remote API", "version": "2.0.0"}, "paths": {}}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(spec)) //nolint:errcheck
	}))
	defer server.Close()

	extr := initExtractor(t, map[string]any{
		"source": server.URL + "/openapi.json",
	})

	emitter := mocks.NewEmitter()
	err := extr.Extract(context.Background(), emitter.Push)
	require.NoError(t, err)

	records := emitter.Get()
	require.Len(t, records, 1)
	assert.Equal(t, "Remote API", records[0].Entity().GetName())
}

func TestServiceNameOverride(t *testing.T) {
	spec := `openapi: "3.0.0"
info:
  title: Original Name
  version: "1.0.0"
paths: {}
`
	dir := t.TempDir()
	specPath := filepath.Join(dir, "spec.yaml")
	require.NoError(t, os.WriteFile(specPath, []byte(spec), 0o644))

	extr := initExtractor(t, map[string]any{
		"source":  specPath,
		"service": "custom-name",
	})

	emitter := mocks.NewEmitter()
	err := extr.Extract(context.Background(), emitter.Push)
	require.NoError(t, err)

	records := emitter.Get()
	require.Len(t, records, 1)
	// The service config overrides the name used in URN but entity name comes from title.
	assert.Contains(t, records[0].Entity().GetUrn(), "custom-name")
}

func TestNoMatchingFiles(t *testing.T) {
	dir := t.TempDir()
	extr := initExtractor(t, map[string]any{
		"source": filepath.Join(dir, "*.nonexistent"),
	})

	emitter := mocks.NewEmitter()
	err := extr.Extract(context.Background(), emitter.Push)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no files matched")
}

// --- helpers ---

func initExtractor(t *testing.T, rawConfig map[string]any) *extractor.Extractor {
	t.Helper()
	extr := extractor.New(testutils.Logger)
	err := extr.Init(context.Background(), plugins.Config{
		URNScope:  urnScope,
		RawConfig: rawConfig,
	})
	require.NoError(t, err)
	return extr
}
