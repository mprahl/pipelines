// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mlflow

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

const (
	DefaultExperimentName = "Default"
	DefaultTimeout        = "30s"
	LauncherConfigMapName = "kfp-launcher"
	LauncherConfigKey     = "plugins.mlflow"
	DefaultTokenKey       = "MLFLOW_TRACKING_TOKEN"
	DefaultUsernameKey    = "MLFLOW_TRACKING_USERNAME"
	DefaultPasswordKey    = "MLFLOW_TRACKING_PASSWORD"
)

// EnvMLflowConfig is the single environment variable injected into Argo
// Workflow templates by the API server.
const EnvMLflowConfig = "KFP_MLFLOW_CONFIG"

// MLflowRuntimeConfig is the JSON payload marshalled into KFP_MLFLOW_CONFIG.
type MLflowRuntimeConfig struct {
	Endpoint           string `json:"endpoint"`
	Workspace          string `json:"workspace,omitempty"`
	ParentRunID        string `json:"parentRunId"`
	ExperimentID       string `json:"experimentId"`
	AuthType           string `json:"authType"`
	Timeout            string `json:"timeout,omitempty"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty"`
}

var ServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// TLSConfig holds TLS settings for the MLflow endpoint.
type TLSConfig struct {
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty" mapstructure:"insecureSkipVerify"`
	CABundlePath       string `json:"caBundlePath,omitempty" mapstructure:"caBundlePath"`
}

// PluginConfig represents the global or namespace-level plugin configuration.
type PluginConfig struct {
	Endpoint string          `json:"endpoint,omitempty" mapstructure:"endpoint"`
	Timeout  string          `json:"timeout,omitempty" mapstructure:"timeout"`
	TLS      *TLSConfig      `json:"tls,omitempty" mapstructure:"tls"`
	Settings json.RawMessage `json:"settings,omitempty" mapstructure:"settings"`
}

// CredentialSecretRef identifies the Kubernetes Secret and keys that hold credentials.
type CredentialSecretRef struct {
	Name        string `json:"name,omitempty"`
	TokenKey    string `json:"tokenKey,omitempty"`
	UsernameKey string `json:"usernameKey,omitempty"`
	PasswordKey string `json:"passwordKey,omitempty"`
}

// MLflowCredentials holds the resolved authentication credentials for an MLflow endpoint.
type MLflowCredentials struct {
	AuthType      string
	BearerToken   string
	BasicUsername string
	BasicPassword string
}

// MergePluginConfig merges namespace-level overrides into the global config.
// The namespace config takes precedence on non-zero fields.
func MergePluginConfig(globalCfg PluginConfig, namespaceCfg *PluginConfig) PluginConfig {
	merged := globalCfg
	if namespaceCfg == nil {
		return merged
	}
	if namespaceCfg.Endpoint != "" {
		merged.Endpoint = namespaceCfg.Endpoint
	}
	if namespaceCfg.Timeout != "" {
		merged.Timeout = namespaceCfg.Timeout
	}
	if namespaceCfg.TLS != nil {
		merged.TLS = namespaceCfg.TLS
	}
	if len(namespaceCfg.Settings) != 0 {
		merged.Settings = namespaceCfg.Settings
	}
	return merged
}

// BuildHTTPClient configures an http.Client with the given timeout and TLS settings.
func BuildHTTPClient(timeout time.Duration, tlsCfg *TLSConfig) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if tlsCfg != nil {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: tlsCfg.InsecureSkipVerify,
		}
		if tlsCfg.CABundlePath != "" {
			caBundle, err := os.ReadFile(tlsCfg.CABundlePath)
			if err != nil {
				return nil, fmt.Errorf("failed to read plugins.mlflow.tls.caBundlePath %q: %w", tlsCfg.CABundlePath, err)
			}
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(caBundle) {
				return nil, fmt.Errorf("plugins.mlflow.tls.caBundlePath %q did not contain valid PEM certificates", tlsCfg.CABundlePath)
			}
			tlsConfig.RootCAs = certPool
		}
		transport.TLSClientConfig = tlsConfig
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}, nil
}

// ToMLflowTerminalStatus converts a KFP RuntimeState string to an MLflow
// terminal status.
func ToMLflowTerminalStatus(stateV2 string) string {
	switch stateV2 {
	case "SUCCEEDED":
		return "FINISHED"
	case "CANCELED", "CANCELING":
		return "KILLED"
	default:
		return "FAILED"
	}
}
