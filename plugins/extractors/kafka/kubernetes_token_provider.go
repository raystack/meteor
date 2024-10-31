package kafka

import (
	"fmt"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
)

const (
	kubernetesServiceAccountTokenPath = "/var/run/secrets/kafka/serviceaccount/token"
)

// NewKubernetesTokenProvider creates a new TokenProvider that reads the token from kubernetes pod service account
// token file. By default, the token file path for kafka is stored in `/var/run/secrets/kafka/serviceaccount/token`.
// User need to make sure there a valid projected service account token on that path.
func NewKubernetesTokenProvider(opts ...TokenProviderOption) *KubernetesTokenProvider {
	options := &TokenProviderOptions{
		FilePath: kubernetesServiceAccountTokenPath,
	}
	for _, o := range opts {
		o(options)
	}
	log.Info().Str("token_file_path", options.FilePath).Msg("token provider options")
	return &KubernetesTokenProvider{
		serviceAccountFilePath: options.FilePath,
	}
}

type KubernetesTokenProvider struct {
	serviceAccountFilePath string
}

// Token returns the token from the service account token file.
func (tp *KubernetesTokenProvider) Token() (*sarama.AccessToken, error) {
	token, err := tp.readFile()
	if err != nil {
		log.Error().Err(err).Msg("failed to read token from service account token file")
		return nil, err
	}
	return &sarama.AccessToken{
		Token: token,
	}, nil
}
func (tp *KubernetesTokenProvider) readFile() (string, error) {
	token, err := os.ReadFile(tp.serviceAccountFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to read files: %w", err)
	}
	tkn := strings.TrimSpace(string(token))
	return tkn, nil
}

type TokenProviderOptions struct {
	// FilePath is the path to the file containing the token.
	FilePath string
}
type TokenProviderOption func(*TokenProviderOptions)

// WithTokenFilePath sets the file path to the token.
func WithTokenFilePath(path string) TokenProviderOption {
	return func(o *TokenProviderOptions) {
		o.FilePath = path
	}
}
