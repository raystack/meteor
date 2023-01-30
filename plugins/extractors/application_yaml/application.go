package applicationyaml

import (
	"time"
)

type Application struct {
	Name string `json:"name" yaml:"name" validate:"required"`
	ID   string `json:"id" yaml:"id" validate:"required"`
	Team struct {
		ID    string `json:"id" yaml:"id"`
		Name  string `json:"name" yaml:"name"`
		Email string `json:"email" yaml:"email"`
	} `json:"team" yaml:"team"`
	Description string            `json:"description" yaml:"description"`
	URL         string            `json:"url" yaml:"url" validate:"omitempty,url"`
	Version     string            `json:"version" yaml:"version"`
	Inputs      []string          `json:"inputs" yaml:"inputs"`
	Outputs     []string          `json:"outputs" yaml:"outputs"`
	CreateTime  time.Time         `json:"create_time" yaml:"create_time"`
	UpdateTime  time.Time         `json:"update_time" yaml:"update_time"`
	Labels      map[string]string `json:"labels" yaml:"labels"`
}
