package kubernetes

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Client talks to the Kubernetes REST API.
type Client struct {
	baseURL    string
	httpClient *http.Client
	token      string
}

// NewClient creates a client from kubeconfig or in-cluster config.
func NewClient(kubeconfigPath string) (*Client, error) {
	if kubeconfigPath != "" {
		return fromKubeconfig(kubeconfigPath)
	}

	// Try KUBECONFIG env.
	if p := os.Getenv("KUBECONFIG"); p != "" {
		return fromKubeconfig(p)
	}

	// Try in-cluster.
	if c, err := fromInCluster(); err == nil {
		return c, nil
	}

	// Try default path.
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("cannot determine home directory: %w", err)
	}
	return fromKubeconfig(filepath.Join(home, ".kube", "config"))
}

func fromInCluster() (*Client, error) {
	const (
		tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
		caPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
		host      = "https://kubernetes.default.svc"
	)

	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caBytes, err := os.ReadFile(caPath); err == nil {
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caBytes)
		tlsCfg.RootCAs = pool
	}

	return &Client{
		baseURL: host,
		token:   string(tokenBytes),
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: tlsCfg},
		},
	}, nil
}

// kubeconfig YAML structures (minimal).
type kubeconfig struct {
	Clusters       []kcCluster `yaml:"clusters"`
	Users          []kcUser    `yaml:"users"`
	Contexts       []kcContext `yaml:"contexts"`
	CurrentContext string      `yaml:"current-context"`
}
type kcCluster struct {
	Name    string `yaml:"name"`
	Cluster struct {
		Server                   string `yaml:"server"`
		CertificateAuthorityData string `yaml:"certificate-authority-data"`
		InsecureSkipTLSVerify    bool   `yaml:"insecure-skip-tls-verify"`
	} `yaml:"cluster"`
}
type kcUser struct {
	Name string `yaml:"name"`
	User struct {
		Token                 string `yaml:"token"`
		ClientCertificateData string `yaml:"client-certificate-data"`
		ClientKeyData         string `yaml:"client-key-data"`
	} `yaml:"user"`
}
type kcContext struct {
	Name    string `yaml:"name"`
	Context struct {
		Cluster string `yaml:"cluster"`
		User    string `yaml:"user"`
	} `yaml:"context"`
}

func fromKubeconfig(path string) (*Client, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read kubeconfig %s: %w", path, err)
	}

	var kc kubeconfig
	if err := yaml.Unmarshal(data, &kc); err != nil {
		return nil, fmt.Errorf("parse kubeconfig: %w", err)
	}

	// Resolve current context.
	var ctxCluster, ctxUser string
	for _, c := range kc.Contexts {
		if c.Name == kc.CurrentContext {
			ctxCluster = c.Context.Cluster
			ctxUser = c.Context.User
			break
		}
	}
	if ctxCluster == "" {
		return nil, fmt.Errorf("current-context %q not found in kubeconfig", kc.CurrentContext)
	}

	var server string
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

	for _, cl := range kc.Clusters {
		if cl.Name == ctxCluster {
			server = cl.Cluster.Server
			if cl.Cluster.InsecureSkipTLSVerify {
				tlsCfg.InsecureSkipVerify = true //nolint:gosec // user-configured
			}
			if ca := cl.Cluster.CertificateAuthorityData; ca != "" {
				caBytes, err := base64.StdEncoding.DecodeString(ca)
				if err == nil {
					pool := x509.NewCertPool()
					pool.AppendCertsFromPEM(caBytes)
					tlsCfg.RootCAs = pool
				}
			}
			break
		}
	}
	if server == "" {
		return nil, fmt.Errorf("cluster %q not found in kubeconfig", ctxCluster)
	}

	var token string
	for _, u := range kc.Users {
		if u.Name == ctxUser {
			token = u.User.Token
			if cert := u.User.ClientCertificateData; cert != "" {
				if key := u.User.ClientKeyData; key != "" {
					certBytes, _ := base64.StdEncoding.DecodeString(cert)
					keyBytes, _ := base64.StdEncoding.DecodeString(key)
					tlsCert, err := tls.X509KeyPair(certBytes, keyBytes)
					if err == nil {
						tlsCfg.Certificates = []tls.Certificate{tlsCert}
					}
				}
			}
			break
		}
	}

	return &Client{
		baseURL: server,
		token:   token,
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: tlsCfg},
		},
	}, nil
}

func (c *Client) get(ctx context.Context, path string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return result, nil
}

func (c *Client) ListNamespaces(ctx context.Context) (map[string]any, error) {
	return c.get(ctx, "/api/v1/namespaces")
}

func (c *Client) ListDeployments(ctx context.Context, namespace string) (map[string]any, error) {
	return c.get(ctx, "/apis/apps/v1/namespaces/"+namespace+"/deployments")
}

func (c *Client) ListServices(ctx context.Context, namespace string) (map[string]any, error) {
	return c.get(ctx, "/api/v1/namespaces/"+namespace+"/services")
}

func (c *Client) ListPods(ctx context.Context, namespace string) (map[string]any, error) {
	return c.get(ctx, "/api/v1/namespaces/"+namespace+"/pods")
}

func (c *Client) ListJobs(ctx context.Context, namespace string) (map[string]any, error) {
	return c.get(ctx, "/apis/batch/v1/namespaces/"+namespace+"/jobs")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
