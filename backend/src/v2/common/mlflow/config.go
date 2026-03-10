package mlflow

import (
	"net/url"
	"time"

	commonmlflow "github.com/kubeflow/pipelines/backend/src/common/mlflow"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	"github.com/kubeflow/pipelines/backend/src/v2/config"
)

type PluginConfig = commonmlflow.PluginConfig
type MLflowCredentials = commonmlflow.MLflowCredentials

const (
	DefaultAuthType   = commonmlflow.DefaultAuthType
	AuthTypeBearer    = commonmlflow.AuthTypeBearer
	AuthTypeBasicAuth = commonmlflow.AuthTypeBasicAuth
)

type TaskInfo struct {
	Workspace         string `json:"workspace"`
	WorkspacesEnabled bool   `json:"workspacesEnabled"`
	ParentRunID       string `json:"parentRunId"`
	RunID             string `json:"childRunId"`
	ExperimentID      string `json:"experimentId"`
	AuthType          string `json:"authType"`
	RunEndTime        int64  `json:"runEndTime"`
	RunStatus         string `json:"runStatus"`
}

// BuildTaskRequestContext constructs a fully initialized RequestContext including
// the underlying MLflow HTTP client with authentication.
func BuildTaskRequestContext(taskInfo TaskInfo, pluginCfg PluginConfig) (*commonmlflow.RequestContext, error) {
	baseURL, err := url.Parse(pluginCfg.Endpoint)
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, util.NewInvalidInputError("invalid plugins.mlflow endpoint %q", pluginCfg.Endpoint)
	}
	timeout, err := time.ParseDuration(pluginCfg.Timeout)
	if err != nil {
		return nil, util.NewInvalidInputError("invalid plugins.mlflow timeout %q: %v", pluginCfg.Timeout, err)
	}
	if timeout <= 0 {
		return nil, util.NewInvalidInputError("plugins.mlflow timeout must be > 0")
	}
	authMaterial, err := ResolveAuthMaterialForTask(taskInfo.AuthType)
	if err != nil {
		return nil, err
	}
	httpClient, err := commonmlflow.BuildHTTPClient(timeout, pluginCfg.TLS)
	if err != nil {
		return nil, err
	}
	retrySettings := commonmlflow.RetryPolicy{
		InitialInterval: commonmlflow.DefaultRetryInitial,
		MaxInterval:     commonmlflow.DefaultRetryMax,
		MaxElapsedTime:  commonmlflow.DefaultRetryElapsed,
		Multiplier:      2.0,
	}
	sharedClient, err := commonmlflow.NewClient(commonmlflow.Config{
		Endpoint:          pluginCfg.Endpoint,
		HTTPClient:        httpClient,
		AuthType:          authMaterial.AuthType,
		BearerToken:       authMaterial.BearerToken,
		BasicAuthUsername: authMaterial.BasicUsername,
		BasicAuthPassword: authMaterial.BasicPassword,
		WorkspacesEnabled: taskInfo.WorkspacesEnabled,
		Workspace:         taskInfo.Workspace,
		Retry:             retrySettings,
	})
	if err != nil {
		return nil, util.NewInvalidInputError("failed to build MLflow client: %v", err)
	}
	return &commonmlflow.RequestContext{
		BaseURL:           baseURL,
		Workspace:         taskInfo.Workspace,
		WorkspacesEnabled: taskInfo.WorkspacesEnabled,
		Client:            sharedClient,
	}, nil
}

func ResolveAuthMaterialForTask(authType string) (MLflowCredentials, error) {
	switch authType {
	case DefaultAuthType:
		return MLflowCredentials{AuthType: authType}, nil
	case AuthTypeBearer:
		return MLflowCredentials{
			AuthType:    authType,
			BearerToken: config.GetMLflowTrackingToken(),
		}, nil
	case AuthTypeBasicAuth:
		return MLflowCredentials{
			AuthType:      authType,
			BasicUsername: config.GetMLflowTrackingUsername(),
			BasicPassword: config.GetMLflowTrackingPassword(),
		}, nil
	default:
		return MLflowCredentials{}, util.NewInvalidInputError("unsupported plugins.mlflow.settings.authType %q (expected one of %q, %q, %q)", authType, DefaultAuthType, AuthTypeBearer, AuthTypeBasicAuth)
	}
}
