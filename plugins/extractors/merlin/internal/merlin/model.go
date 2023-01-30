package merlin

import "time"

type Model struct {
	ID                 int64           `json:"id"`
	ProjectID          int64           `json:"project_id"`
	MlflowExperimentID int64           `json:"mlflow_experiment_id"`
	Name               string          `json:"name"`
	Type               string          `json:"type"`
	MlflowURL          string          `json:"mlflow_url"`
	Endpoints          []ModelEndpoint `json:"endpoints"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
}

type ModelEndpoint struct {
	ID              int64             `json:"id"`
	Status          string            `json:"status"` // pending/running/serving/failed/terminated
	URL             string            `json:"url"`
	Rule            ModelEndpointRule `json:"rule"`
	EnvironmentName string            `json:"environment_name"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	// ModelID      int64              `json:"model_id"`
	// Environment  *Environment       `json:"environment"`
}

type ModelEndpointRule struct {
	Destinations []ModelEndpointRuleDestination `json:"destinations"`
	// Mirror    *VersionEndpoint               `json:"mirror"`
}

type ModelEndpointRuleDestination struct {
	VersionEndpointID string           `json:"version_endpoint_id"`
	VersionEndpoint   *VersionEndpoint `json:"version_endpoint"`
	Weight            int64            `json:"weight"`
}

type VersionEndpoint struct {
	ID              string      `json:"id"`
	VersionID       int64       `json:"version_id"`
	Status          string      `json:"status"` // pending/running/serving/failed/terminated
	URL             string      `json:"url"`
	ServiceName     string      `json:"service_name"`
	EnvironmentName string      `json:"environment_name"`
	MonitoringURL   string      `json:"monitoring_url"`
	Message         string      `json:"message"`
	EnvVars         []EnvVar    `json:"env_vars"`
	Transformer     Transformer `json:"transformer"`
	DeploymentMode  string      `json:"deployment_mode"` // serverless/raw_deployment
	CreatedAt       time.Time   `json:"created_at"`
	UpdatedAt       time.Time   `json:"updated_at"`
	// Environment       *Environment       `json:"environment"`
	// ResourceRequest   *ResourceRequest   `json:"resource_request"`
	// AutoscalingPolicy *AutoscalingPolicy `json:"autoscaling_policy"`
	// Logger            *Logger            `json:"logger"`
}

type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Transformer struct {
	Enabled         bool      `json:"enabled"`
	TransformerType string    `json:"transformer_type"`
	Image           string    `json:"image"`
	Command         string    `json:"command"`
	Args            string    `json:"args"`
	EnvVars         []EnvVar  `json:"env_vars"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	// ResourceRequest *ResourceRequest `json:"resource_request"`
}
