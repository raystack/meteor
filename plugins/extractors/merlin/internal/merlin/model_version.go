package merlin

import "time"

type ModelVersion struct {
	ID          int64             `json:"id"`
	ModelD      int64             `json:"model_id"`
	MlflowRunID string            `json:"mlflow_run_id"`
	MlflowURL   string            `json:"mlflow_url"`
	Endpoints   []VersionEndpoint `json:"endpoints"`
	Labels      map[string]string `json:"labels"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	// ArtifactURI     string           `json:"artifact_uri"`
	// Properties      interface{}      `json:"properties"`
	// CustomPredictor *CustomPredictor `json:"custom_predictor"`
}
