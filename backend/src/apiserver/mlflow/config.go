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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	commonmlflow "github.com/kubeflow/pipelines/backend/src/common/mlflow"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type PluginConfig = commonmlflow.PluginConfig
type TLSConfig = commonmlflow.TLSConfig
type CredentialSecretRef = commonmlflow.CredentialSecretRef
type MLflowCredentials = commonmlflow.MLflowCredentials

// MLflowPluginSettings contains MLflow-specific settings parsed from PluginConfig.Settings.
type MLflowPluginSettings struct {
	WorkspacesEnabled     *bool                `json:"workspacesEnabled,omitempty"`
	AuthType              string               `json:"authType,omitempty"`
	CredentialSecretRef   *CredentialSecretRef `json:"credentialSecretRef,omitempty"`
	ExperimentDescription *string              `json:"experimentDescription,omitempty"`
}

// ParseMLflowPluginSettings deserializes raw settings JSON with defaults.
func ParseMLflowPluginSettings(raw json.RawMessage) (*MLflowPluginSettings, error) {
	settings := &MLflowPluginSettings{
		AuthType: commonmlflow.DefaultAuthType,
	}
	if len(raw) != 0 {
		if err := json.Unmarshal(raw, settings); err != nil {
			return nil, fmt.Errorf("failed to parse plugins.mlflow.settings: %w", err)
		}
	}
	if settings.AuthType == "" {
		settings.AuthType = commonmlflow.DefaultAuthType
	}
	if settings.WorkspacesEnabled == nil {
		defaultEnabled := settings.AuthType == commonmlflow.DefaultAuthType
		settings.WorkspacesEnabled = &defaultEnabled
	}
	return settings, nil
}

// ResolvedConfig bundles the merged plugin configuration and its parsed settings.
type ResolvedConfig struct {
	Settings *MLflowPluginSettings
	Config   *PluginConfig
}

// MLflowPluginInput represents the user-facing plugins_input.mlflow schema.
type MLflowPluginInput struct {
	ExperimentName string `json:"experiment_name,omitempty"`
	ExperimentID   string `json:"experiment_id,omitempty"`
	Disabled       bool   `json:"disabled,omitempty"`
}

// KubeClientProvider abstracts Kubernetes clientset access.
type KubeClientProvider interface {
	GetClientSet() kubernetes.Interface
}

// GetGlobalMLflowConfig reads the global plugins.mlflow configuration
func GetGlobalMLflowConfig() (PluginConfig, bool, error) {
	if !viper.IsSet("plugins.mlflow") {
		return PluginConfig{}, false, nil
	}
	raw := viper.Get("plugins.mlflow")
	data, err := json.Marshal(raw)
	if err != nil {
		return PluginConfig{}, false, util.NewInvalidInputError("failed to marshal global plugins.mlflow config: %v", err)
	}
	var cfg PluginConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return PluginConfig{}, false, util.NewInvalidInputError("failed to parse global plugins.mlflow config: %v", err)
	}
	return cfg, true, nil
}

// GetNamespaceMLflowConfig reads the namespace-level MLflow configuration
// from the kfp-launcher ConfigMap.
func GetNamespaceMLflowConfig(ctx context.Context, clientSet kubernetes.Interface, namespace string) (*PluginConfig, bool, error) {
	if namespace == "" || clientSet == nil {
		return nil, false, nil
	}
	cm, err := clientSet.CoreV1().ConfigMaps(namespace).Get(ctx, commonmlflow.LauncherConfigMapName, v1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, util.NewInternalServerError(err, "failed to read namespace configmap %q in namespace %q", commonmlflow.LauncherConfigMapName, namespace)
	}
	raw, ok := cm.Data[commonmlflow.LauncherConfigKey]
	if !ok || raw == "" {
		return nil, false, nil
	}
	var cfg PluginConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, false, util.NewInvalidInputError("failed to parse namespace %q %s as JSON: %v", namespace, commonmlflow.LauncherConfigKey, err)
	}
	return &cfg, true, nil
}

// ResolveMLflowRequestConfig builds a merged and validated ResolvedConfig for the
// given namespace, combining global and namespace-level configuration.
func ResolveMLflowRequestConfig(ctx context.Context, kubeClients KubeClientProvider, namespace string) (*ResolvedConfig, error) {
	globalCfg, hasGlobal, err := GetGlobalMLflowConfig()
	if err != nil {
		return nil, err
	}

	var clientSet kubernetes.Interface
	if kubeClients != nil {
		clientSet = kubeClients.GetClientSet()
	}
	namespaceCfg, hasNamespace, err := GetNamespaceMLflowConfig(ctx, clientSet, namespace)
	if err != nil {
		return nil, err
	}
	if !hasGlobal && !hasNamespace {
		return nil, nil
	}
	mergedCfg := commonmlflow.MergePluginConfig(globalCfg, namespaceCfg)
	if mergedCfg.Timeout == "" {
		mergedCfg.Timeout = commonmlflow.DefaultTimeout
	}
	settings, err := ParseMLflowPluginSettings(mergedCfg.Settings)
	if err != nil {
		return nil, err
	}
	if hasGlobal && hasNamespace && namespaceCfg != nil && namespaceCfg.Endpoint != "" && globalCfg.Endpoint != namespaceCfg.Endpoint {
		if settings.CredentialSecretRef == nil || settings.CredentialSecretRef.Name == "" {
			return nil, util.NewInvalidInputError("namespace plugins.mlflow endpoint override requires namespace credentialSecretRef")
		}
	}
	return &ResolvedConfig{
		Settings: settings,
		Config:   &mergedCfg,
	}, nil
}

// ResolveMLflowCredentials resolves the authentication credentials for the MLflow
// endpoint based on the configured authType.
func ResolveMLflowCredentials(ctx context.Context, clientSet kubernetes.Interface, namespace string, settings *MLflowPluginSettings) (MLflowCredentials, error) {
	authType := commonmlflow.DefaultAuthType
	if settings != nil && settings.AuthType != "" {
		authType = strings.ToLower(settings.AuthType)
	}
	switch authType {
	case commonmlflow.DefaultAuthType:
		tokenBytes, err := os.ReadFile(commonmlflow.ServiceAccountTokenPath)
		if err != nil {
			return MLflowCredentials{}, util.NewInternalServerError(err, "failed to read API server service account token for MLflow auth")
		}
		token := strings.TrimSpace(string(tokenBytes))
		if token == "" {
			return MLflowCredentials{}, util.NewInvalidInputError("API server service account token is empty for MLflow auth")
		}
		return MLflowCredentials{
			AuthType:    authType,
			BearerToken: token,
		}, nil
	case commonmlflow.AuthTypeBearer:
		secretData, secretRef, err := GetMLflowCredentialSecretData(ctx, clientSet, namespace, settings, authType)
		if err != nil {
			return MLflowCredentials{}, err
		}
		tokenKey := secretRef.TokenKey
		if tokenKey == "" {
			tokenKey = commonmlflow.DefaultTokenKey
		}
		token := strings.TrimSpace(string(secretData[tokenKey]))
		if token == "" {
			return MLflowCredentials{}, util.NewInvalidInputError("plugins.mlflow credential secret %q key %q must be non-empty for bearer auth", secretRef.Name, tokenKey)
		}
		return MLflowCredentials{
			AuthType:    authType,
			BearerToken: token,
		}, nil
	case commonmlflow.AuthTypeBasicAuth:
		secretData, secretRef, err := GetMLflowCredentialSecretData(ctx, clientSet, namespace, settings, authType)
		if err != nil {
			return MLflowCredentials{}, err
		}
		usernameKey := secretRef.UsernameKey
		if usernameKey == "" {
			usernameKey = commonmlflow.DefaultUsernameKey
		}
		passwordKey := secretRef.PasswordKey
		if passwordKey == "" {
			passwordKey = commonmlflow.DefaultPasswordKey
		}
		username := strings.TrimSpace(string(secretData[usernameKey]))
		password := strings.TrimSpace(string(secretData[passwordKey]))
		if username == "" {
			return MLflowCredentials{}, util.NewInvalidInputError("plugins.mlflow credential secret %q key %q must be non-empty for basic-auth", secretRef.Name, usernameKey)
		}
		if password == "" {
			return MLflowCredentials{}, util.NewInvalidInputError("plugins.mlflow credential secret %q key %q must be non-empty for basic-auth", secretRef.Name, passwordKey)
		}
		return MLflowCredentials{
			AuthType:      authType,
			BasicUsername: username,
			BasicPassword: password,
		}, nil
	default:
		return MLflowCredentials{}, util.NewInvalidInputError("unsupported plugins.mlflow.settings.authType %q (expected one of %q, %q, %q)", authType, commonmlflow.DefaultAuthType, commonmlflow.AuthTypeBearer, commonmlflow.AuthTypeBasicAuth)
	}
}

// GetMLflowCredentialSecretData reads and returns the data from the credential Secret
// referenced in settings.
func GetMLflowCredentialSecretData(ctx context.Context, clientSet kubernetes.Interface, namespace string, settings *MLflowPluginSettings, authType string) (map[string][]byte, *CredentialSecretRef, error) {
	if namespace == "" {
		return nil, nil, util.NewInvalidInputError("namespace must be set for MLflow %s auth", authType)
	}
	if settings == nil || settings.CredentialSecretRef == nil || settings.CredentialSecretRef.Name == "" {
		return nil, nil, util.NewInvalidInputError("plugins.mlflow.settings.credentialSecretRef.name is required for authType %q", authType)
	}
	if clientSet == nil {
		return nil, nil, util.NewInternalServerError(errors.New("kubernetes clientset is nil"), "failed to read MLflow credential secret")
	}
	secretRef := settings.CredentialSecretRef
	secret, err := clientSet.CoreV1().Secrets(namespace).Get(ctx, secretRef.Name, v1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, util.NewInvalidInputError("plugins.mlflow credential secret %q not found in namespace %q", secretRef.Name, namespace)
		}
		return nil, nil, util.NewInternalServerError(err, "failed to read MLflow credential secret %q in namespace %q", secretRef.Name, namespace)
	}
	return secret.Data, secretRef, nil
}

// BuildMLflowRequestContext constructs a fully initialized RequestContext including
// the underlying MLflow HTTP client with authentication.
func BuildMLflowRequestContext(ctx context.Context, clientSet kubernetes.Interface, namespace string, requestCfg *ResolvedConfig) (*RequestContext, error) {
	if requestCfg == nil || requestCfg.Config == nil || requestCfg.Config.Endpoint == "" {
		return nil, nil
	}
	baseURL, err := url.Parse(requestCfg.Config.Endpoint)
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, util.NewInvalidInputError("invalid plugins.mlflow endpoint %q", requestCfg.Config.Endpoint)
	}
	settings := requestCfg.Settings
	if settings == nil {
		settings, err = ParseMLflowPluginSettings(requestCfg.Config.Settings)
		if err != nil {
			return nil, err
		}
	}
	timeout, err := time.ParseDuration(requestCfg.Config.Timeout)
	if err != nil {
		return nil, util.NewInvalidInputError("invalid plugins.mlflow timeout %q: %v", requestCfg.Config.Timeout, err)
	}
	if timeout <= 0 {
		return nil, util.NewInvalidInputError("plugins.mlflow timeout must be > 0")
	}
	authMaterial, err := ResolveMLflowCredentials(ctx, clientSet, namespace, settings)
	if err != nil {
		return nil, err
	}
	httpClient, err := commonmlflow.BuildHTTPClient(timeout, requestCfg.Config.TLS)
	if err != nil {
		return nil, err
	}
	workspacesEnabled := settings.WorkspacesEnabled != nil && *settings.WorkspacesEnabled
	retrySettings := commonmlflow.RetryPolicy{
		InitialInterval: commonmlflow.DefaultRetryInitial,
		MaxInterval:     commonmlflow.DefaultRetryMax,
		MaxElapsedTime:  commonmlflow.DefaultRetryElapsed,
		Multiplier:      2.0,
	}
	sharedClient, err := commonmlflow.NewClient(commonmlflow.Config{
		Endpoint:          requestCfg.Config.Endpoint,
		HTTPClient:        httpClient,
		AuthType:          authMaterial.AuthType,
		BearerToken:       authMaterial.BearerToken,
		BasicAuthUsername: authMaterial.BasicUsername,
		BasicAuthPassword: authMaterial.BasicPassword,
		WorkspacesEnabled: workspacesEnabled,
		Workspace:         namespace,
		Retry:             retrySettings,
	})
	if err != nil {
		return nil, util.NewInvalidInputError("failed to build MLflow client: %v", err)
	}
	return &RequestContext{
		BaseURL:           baseURL,
		Workspace:         namespace,
		WorkspacesEnabled: workspacesEnabled,
		Client:            sharedClient,
	}, nil
}

// ResolveMLflowPluginInput parses the plugins_input.mlflow JSON from a run model,
// validates it against the MLflowPluginInput schema, and applies defaults.
func ResolveMLflowPluginInput(pluginsInputString *string) (*MLflowPluginInput, error) {
	if pluginsInputString == nil || *pluginsInputString == "" {
		return &MLflowPluginInput{ExperimentName: commonmlflow.DefaultExperimentName}, nil
	}

	pluginInputs := map[string]json.RawMessage{}
	if err := json.Unmarshal([]byte(*pluginsInputString), &pluginInputs); err != nil {
		return nil, util.NewInvalidInputError("plugins_input must be a valid JSON object: %v", err)
	}
	mlflowRaw, ok := pluginInputs["mlflow"]
	if !ok || len(mlflowRaw) == 0 {
		return &MLflowPluginInput{ExperimentName: commonmlflow.DefaultExperimentName}, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(mlflowRaw))
	decoder.DisallowUnknownFields()
	input := &MLflowPluginInput{}
	if err := decoder.Decode(input); err != nil {
		return nil, util.NewInvalidInputError("plugins_input.mlflow must follow schema {experiment_name?: string, experiment_id?: string, disabled?: bool}: %v", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, util.NewInvalidInputError("plugins_input.mlflow must be a single JSON object")
	}

	if input.Disabled {
		return input, nil
	}
	if input.ExperimentID != "" {
		return input, nil
	}
	if input.ExperimentName == "" {
		input.ExperimentName = commonmlflow.DefaultExperimentName
	}
	return input, nil
}

// SelectMLflowExperiment chooses the selector used for MLflow experiment resolution.
// experiment_id takes precedence over experiment_name.
func SelectMLflowExperiment(input *MLflowPluginInput) (experimentID string, experimentName string) {
	if input == nil {
		return "", commonmlflow.DefaultExperimentName
	}
	if input.ExperimentID != "" {
		return input.ExperimentID, ""
	}
	if input.ExperimentName == "" {
		return "", commonmlflow.DefaultExperimentName
	}
	return "", input.ExperimentName
}

// InjectMLflowRuntimeEnv injects the KFP_MLFLOW_CONFIG env var and (when
// mlflowEnabled is true) the --mlflow_enabled flag into driver and launcher
// Argo Workflow container templates.
func InjectMLflowRuntimeEnv(executionSpec util.ExecutionSpec, env map[string]string, mlflowEnabled bool) error {
	if len(env) == 0 || executionSpec == nil {
		return nil
	}
	workflow, ok := executionSpec.(*util.Workflow)
	if !ok {
		return util.NewInternalServerError(errors.New("unsupported execution spec type"), "failed to inject MLflow runtime env: unsupported execution spec")
	}
	workflow.SetEnvVarsToDriverAndLauncherTemplates(env)
	if mlflowEnabled {
		workflow.SetMLflowEnabledFlag()
	}
	return nil
}
