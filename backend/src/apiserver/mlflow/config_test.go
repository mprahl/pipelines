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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	workflowapi "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	apiv2beta1 "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	commonmlflow "github.com/kubeflow/pipelines/backend/src/common/mlflow"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

// setupSATokenFile writes content to a temp file and overrides ServiceAccountTokenPath
// for the duration of the test.
func setupSATokenFile(t *testing.T, content string) {
	t.Helper()
	tokenFile, err := os.CreateTemp(t.TempDir(), "sa-token-*")
	require.NoError(t, err)
	_, err = tokenFile.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, tokenFile.Close())
	orig := commonmlflow.ServiceAccountTokenPath
	commonmlflow.ServiceAccountTokenPath = tokenFile.Name()
	t.Cleanup(func() { commonmlflow.ServiceAccountTokenPath = orig })
}

func TestResolveMLflowPluginInput(t *testing.T) {
	tests := []struct {
		name      string
		input     *string
		want      *MLflowPluginInput
		wantError bool
	}{
		{
			name:  "nil input defaults",
			input: nil,
			want:  &MLflowPluginInput{ExperimentName: commonmlflow.DefaultExperimentName},
		},
		{
			name:  "empty string defaults",
			input: strPtr(""),
			want:  &MLflowPluginInput{ExperimentName: commonmlflow.DefaultExperimentName},
		},
		{
			name:  "missing mlflow block defaults",
			input: strPtr(`{"other":{"x":"y"}}`),
			want:  &MLflowPluginInput{ExperimentName: commonmlflow.DefaultExperimentName},
		},
		{
			name:  "missing experiment_name defaults",
			input: strPtr(`{"mlflow":{"disabled":false}}`),
			want:  &MLflowPluginInput{ExperimentName: commonmlflow.DefaultExperimentName},
		},
		{
			name:  "valid experiment_name is used",
			input: strPtr(`{"mlflow":{"experiment_name":"exp-1"}}`),
			want:  &MLflowPluginInput{ExperimentName: "exp-1"},
		},
		{
			name:  "experiment_id takes precedence and is preserved",
			input: strPtr(`{"mlflow":{"experiment_name":"exp-1","experiment_id":"42"}}`),
			want:  &MLflowPluginInput{ExperimentName: "exp-1", ExperimentID: "42"},
		},
		{
			name:  "disabled true is accepted",
			input: strPtr(`{"mlflow":{"disabled":true}}`),
			want:  &MLflowPluginInput{Disabled: true},
		},
		{
			name:      "invalid plugins_input json errors",
			input:     strPtr(`{`),
			wantError: true,
		},
		{
			name:      "non-string experiment_name errors",
			input:     strPtr(`{"mlflow":{"experiment_name":123}}`),
			wantError: true,
		},
		{
			name:      "non-string experiment_id errors",
			input:     strPtr(`{"mlflow":{"experiment_id":123}}`),
			wantError: true,
		},
		{
			name:      "unknown field is rejected",
			input:     strPtr(`{"mlflow":{"experiment_name":"exp-1","unknown":"x"}}`),
			wantError: true,
		},
		{
			name:      "mlflow block must be an object",
			input:     strPtr(`{"mlflow":"not-an-object"}`),
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveMLflowPluginInput(tt.input)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSelectMLflowExperiment(t *testing.T) {
	tests := []struct {
		name               string
		input              *MLflowPluginInput
		wantExperimentID   string
		wantExperimentName string
	}{
		{
			name:               "nil input defaults to Default by name",
			input:              nil,
			wantExperimentID:   "",
			wantExperimentName: commonmlflow.DefaultExperimentName,
		},
		{
			name:               "experiment_id takes precedence",
			input:              &MLflowPluginInput{ExperimentID: "42", ExperimentName: "ignored"},
			wantExperimentID:   "42",
			wantExperimentName: "",
		},
		{
			name:               "uses experiment_name when id absent",
			input:              &MLflowPluginInput{ExperimentName: "exp-a"},
			wantExperimentID:   "",
			wantExperimentName: "exp-a",
		},
		{
			name:               "empty selector defaults to Default by name",
			input:              &MLflowPluginInput{},
			wantExperimentID:   "",
			wantExperimentName: commonmlflow.DefaultExperimentName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, gotName := SelectMLflowExperiment(tt.input)
			assert.Equal(t, tt.wantExperimentID, gotID)
			assert.Equal(t, tt.wantExperimentName, gotName)
		})
	}
}

func TestGetGlobalMLflowConfig(t *testing.T) {
	originalPlugins := viper.Get("plugins")
	t.Cleanup(func() {
		viper.Set("plugins", originalPlugins)
	})

	viper.Set("plugins", map[string]interface{}{
		"mlflow": map[string]interface{}{
			"endpoint": "https://mlflow.example.com",
			"timeout":  "10s",
			"settings": map[string]interface{}{
				"authType":          "basic-auth",
				"workspacesEnabled": false,
			},
		},
	})

	cfg, ok, err := GetGlobalMLflowConfig()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "https://mlflow.example.com", cfg.Endpoint)
	assert.Equal(t, "10s", cfg.Timeout)
	require.NotEmpty(t, cfg.Settings)
}

func TestMergePluginConfigAndSettingsDefaults(t *testing.T) {
	global := PluginConfig{
		Endpoint: "https://global-mlflow.example.com",
		Timeout:  "30s",
		Settings: json.RawMessage(`{"authType":"kubernetes"}`),
	}
	namespace := &PluginConfig{
		Endpoint: "https://ns-mlflow.example.com",
		Settings: json.RawMessage(`{"authType":"basic-auth","credentialSecretRef":{"name":"mlflow-creds"}}`),
	}

	merged := commonmlflow.MergePluginConfig(global, namespace)
	assert.Equal(t, "https://ns-mlflow.example.com", merged.Endpoint)
	assert.Equal(t, "30s", merged.Timeout)

	settings, err := ParseMLflowPluginSettings(merged.Settings)
	require.NoError(t, err)
	require.NotNil(t, settings)
	assert.Equal(t, "basic-auth", settings.AuthType)
	require.NotNil(t, settings.CredentialSecretRef)
	assert.Equal(t, "mlflow-creds", settings.CredentialSecretRef.Name)
	require.NotNil(t, settings.WorkspacesEnabled)
	assert.False(t, *settings.WorkspacesEnabled)
}

func TestBuildMLflowRequestContextKubernetesAuth(t *testing.T) {
	setupSATokenFile(t, "sa-token-value\n")

	workspacesEnabled := true
	requestCfg := &ResolvedConfig{
		Config: &PluginConfig{
			Endpoint: "https://mlflow.example.com",
			Timeout:  "12s",
		},
		Settings: &MLflowPluginSettings{
			AuthType:          commonmlflow.DefaultAuthType,
			WorkspacesEnabled: &workspacesEnabled,
		},
	}

	mlflowCtx, err := BuildMLflowRequestContext(context.Background(), nil, "ns1", requestCfg)
	require.NoError(t, err)
	require.NotNil(t, mlflowCtx)
	assert.Equal(t, "https://mlflow.example.com", mlflowCtx.BaseURL.String())
	assert.True(t, mlflowCtx.WorkspacesEnabled)
	assert.NotNil(t, mlflowCtx.Client)
}

func TestBuildMLflowRequestContextSecretBasedAuth(t *testing.T) {
	clientSet := k8sfake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: v1.ObjectMeta{
				Name:      "mlflow-creds",
				Namespace: "ns1",
			},
			Data: map[string][]byte{
				"token":    []byte("bearer-secret"),
				"username": []byte("mlflow-user"),
				"password": []byte("mlflow-pass"),
			},
		},
	)

	t.Run("bearer auth", func(t *testing.T) {
		workspacesEnabled := true
		requestCfg := &ResolvedConfig{
			Config: &PluginConfig{
				Endpoint: "https://mlflow.example.com",
				Timeout:  "30s",
			},
			Settings: &MLflowPluginSettings{
				AuthType: commonmlflow.AuthTypeBearer,
				CredentialSecretRef: &CredentialSecretRef{
					Name:     "mlflow-creds",
					TokenKey: "token",
				},
				WorkspacesEnabled: &workspacesEnabled,
			},
		}
		mlflowCtx, err := BuildMLflowRequestContext(context.Background(), clientSet, "ns1", requestCfg)
		require.NoError(t, err)
		assert.NotNil(t, mlflowCtx)
		assert.True(t, mlflowCtx.WorkspacesEnabled)
		assert.NotNil(t, mlflowCtx.Client)
	})

	t.Run("basic auth", func(t *testing.T) {
		workspacesEnabled := false
		requestCfg := &ResolvedConfig{
			Config: &PluginConfig{
				Endpoint: "https://mlflow.example.com",
				Timeout:  "30s",
			},
			Settings: &MLflowPluginSettings{
				AuthType: commonmlflow.AuthTypeBasicAuth,
				CredentialSecretRef: &CredentialSecretRef{
					Name:        "mlflow-creds",
					UsernameKey: "username",
					PasswordKey: "password",
				},
				WorkspacesEnabled: &workspacesEnabled,
			},
		}
		mlflowCtx, err := BuildMLflowRequestContext(context.Background(), clientSet, "ns1", requestCfg)
		require.NoError(t, err)
		assert.NotNil(t, mlflowCtx)
		assert.False(t, mlflowCtx.WorkspacesEnabled)
		assert.NotNil(t, mlflowCtx.Client)
	})
}

func TestBuildMLflowRequestContextMissingSecretRefValidation(t *testing.T) {
	requestCfg := &ResolvedConfig{
		Config: &PluginConfig{
			Endpoint: "https://mlflow.example.com",
			Timeout:  "30s",
		},
		Settings: &MLflowPluginSettings{
			AuthType: commonmlflow.AuthTypeBearer,
		},
	}
	_, err := BuildMLflowRequestContext(context.Background(), nil, "ns1", requestCfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "credentialSecretRef.name is required")
}

func TestEnsureExperimentExists(t *testing.T) {
	t.Run("returns existing experiment from get-by-name", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "Bearer bearer-secret", r.Header.Get("Authorization"))
			assert.Equal(t, "ns1", r.Header.Get("X-MLflow-Workspace"))
			assert.Equal(t, "/api/2.0/mlflow/experiments/get-by-name", r.URL.Path)
			assert.Equal(t, "my-exp", r.URL.Query().Get("experiment_name"))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"experiment":{"experiment_id":"42","name":"my-exp"}}`))
		}))
		defer server.Close()

		mlflowCtx := newTestMLflowRequestContext(t, server.URL)
		exp, err := EnsureExperimentExists(context.Background(), mlflowCtx, "", "my-exp", ResolveExperimentDescription(nil))
		require.NoError(t, err)
		require.NotNil(t, exp)
		assert.Equal(t, "42", exp.ID)
		assert.Equal(t, "my-exp", exp.Name)
	})

	t.Run("creates experiment when get-by-name returns not found", func(t *testing.T) {
		var callCount int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "Bearer bearer-secret", r.Header.Get("Authorization"))
			assert.Equal(t, "ns1", r.Header.Get("X-MLflow-Workspace"))
			switch r.URL.Path {
			case "/api/2.0/mlflow/experiments/get-by-name":
				callCount++
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error_code":"RESOURCE_DOES_NOT_EXIST","message":"not found"}`))
			case "/api/2.0/mlflow/experiments/create":
				bodyBytes, _ := io.ReadAll(r.Body)
				assert.Contains(t, string(bodyBytes), `"name":"my-exp"`)
				assert.Contains(t, string(bodyBytes), `"description":"Created by Kubeflow Pipelines"`)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"experiment_id":"99"}`))
			default:
				t.Fatalf("unexpected path %s", r.URL.Path)
			}
		}))
		defer server.Close()

		mlflowCtx := newTestMLflowRequestContext(t, server.URL)
		exp, err := EnsureExperimentExists(context.Background(), mlflowCtx, "", "my-exp", ResolveExperimentDescription(nil))
		require.NoError(t, err)
		require.NotNil(t, exp)
		assert.Equal(t, "99", exp.ID)
		assert.Equal(t, "my-exp", exp.Name)
		assert.Equal(t, 1, callCount)
	})
}

func TestCreateParentRunAndTagWithKFPMetadata(t *testing.T) {
	type setTagRequest struct {
		RunID string `json:"run_id"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	var seenTagPayloads []setTagRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer bearer-secret", r.Header.Get("Authorization"))
		assert.Equal(t, "ns1", r.Header.Get("X-MLflow-Workspace"))
		switch r.URL.Path {
		case "/api/2.0/mlflow/runs/create":
			body, _ := io.ReadAll(r.Body)
			assert.Contains(t, string(body), `"experiment_id":"exp-1"`)
			assert.Contains(t, string(body), `"run_name":"sample-run"`)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"run":{"info":{"run_id":"mlflow-parent-run-1"}}}`))
		case "/api/2.0/mlflow/runs/set-tag":
			var tagReq setTagRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&tagReq))
			seenTagPayloads = append(seenTagPayloads, tagReq)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	mlflowCtx := newTestMLflowRequestContext(t, server.URL)

	mlflowRunID, err := mlflowCtx.Client.CreateRun(context.Background(), "exp-1", "sample-run", nil)
	require.NoError(t, err)
	assert.Equal(t, "mlflow-parent-run-1", mlflowRunID)

	run := &apiv2beta1.Run{
		RunId: "kfp-run-1",
		PipelineSource: &apiv2beta1.Run_PipelineVersionReference{
			PipelineVersionReference: &apiv2beta1.PipelineVersionReference{
				PipelineId:        "pipeline-1",
				PipelineVersionId: "pipeline-version-1",
			},
		},
	}
	err = TagRunWithKFPMetadata(context.Background(), mlflowCtx, mlflowRunID, run)
	require.NoError(t, err)

	require.Len(t, seenTagPayloads, 4)
	assert.Contains(t, seenTagPayloads, setTagRequest{
		RunID: "mlflow-parent-run-1",
		Key:   TagKFPRunID,
		Value: "kfp-run-1",
	})
	assert.Contains(t, seenTagPayloads, setTagRequest{
		RunID: "mlflow-parent-run-1",
		Key:   TagKFPRunURL,
		Value: "/#/runs/details/kfp-run-1",
	})
	assert.Contains(t, seenTagPayloads, setTagRequest{
		RunID: "mlflow-parent-run-1",
		Key:   TagKFPPipelineID,
		Value: "pipeline-1",
	})
	assert.Contains(t, seenTagPayloads, setTagRequest{
		RunID: "mlflow-parent-run-1",
		Key:   TagKFPPipelineVersionID,
		Value: "pipeline-version-1",
	})
}

func TestBuildPluginOutput(t *testing.T) {
	output := SuccessfulPluginOutput("exp-1", "my-exp", "run-1", "https://mlflow.example/runs/run-1")
	require.NotNil(t, output)
	assert.Equal(t, apiv2beta1.PluginState_PLUGIN_SUCCEEDED, output.State)
	require.Contains(t, output.Entries, "run_url")
	require.NotNil(t, output.Entries["run_url"].RenderType)
	assert.Equal(t, apiv2beta1.MetadataValue_URL, *output.Entries["run_url"].RenderType)
}

func TestSetMLflowPluginOutput(t *testing.T) {
	lt := model.LargeText(`{"other":{"state":"PLUGIN_SUCCEEDED"}}`)
	run := &model.Run{
		RunDetails: model.RunDetails{
			PluginsOutputString: &lt,
		},
	}
	mlflowOutput := SuccessfulPluginOutput("exp-1", "my-exp", "run-1", "https://mlflow.example/runs/run-1")
	err := SetMLflowPluginOutput(run, "mlflow", mlflowOutput)
	require.NoError(t, err)
	require.NotNil(t, run.PluginsOutputString)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(*run.PluginsOutputString), &raw))
	assert.Contains(t, raw, "other")
	assert.Contains(t, raw, "mlflow")
	var parsed apiv2beta1.PluginOutput
	require.NoError(t, protojson.Unmarshal(raw["mlflow"], &parsed))
	assert.Equal(t, apiv2beta1.PluginState_PLUGIN_SUCCEEDED, parsed.State)
	assert.Contains(t, parsed.Entries, "experiment_id")
}

func TestToMLflowTerminalStatus(t *testing.T) {
	assert.Equal(t, "FINISHED", commonmlflow.ToMLflowTerminalStatus("SUCCEEDED"))
	assert.Equal(t, "KILLED", commonmlflow.ToMLflowTerminalStatus("CANCELED"))
	assert.Equal(t, "KILLED", commonmlflow.ToMLflowTerminalStatus("CANCELING"))
	assert.Equal(t, "FAILED", commonmlflow.ToMLflowTerminalStatus("FAILED"))
	assert.Equal(t, "FAILED", commonmlflow.ToMLflowTerminalStatus("UNKNOWN"))
}

func strPtr(s string) *string {
	return &s
}

// fakeKubeClientProvider implements KubeClientProvider for tests.
type fakeKubeClientProvider struct {
	clientSet kubernetes.Interface
}

func (f *fakeKubeClientProvider) GetClientSet() kubernetes.Interface {
	return f.clientSet
}

// TestResolveMLflowRequestConfig_NamespaceOnlyEnablement verifies that MLflow is
// enabled when no global plugins.mlflow config exists, but the namespace has a
// valid kfp-launcher ConfigMap with plugins.mlflow configuration.
func TestResolveMLflowRequestConfig_NamespaceOnlyEnablement(t *testing.T) {
	// Ensure no global config.
	originalPlugins := viper.Get("plugins")
	viper.Set("plugins", nil)
	t.Cleanup(func() {
		viper.Set("plugins", originalPlugins)
	})

	// Verify global is absent.
	_, hasGlobal, err := GetGlobalMLflowConfig()
	require.NoError(t, err)
	require.False(t, hasGlobal, "global config should be absent for this test")

	// Create a fake namespace ConfigMap with MLflow config.
	clientSet := k8sfake.NewSimpleClientset(
		&corev1.ConfigMap{
			ObjectMeta: v1.ObjectMeta{
				Name:      commonmlflow.LauncherConfigMapName,
				Namespace: "ns-only",
			},
			Data: map[string]string{
				commonmlflow.LauncherConfigKey: `{
					"endpoint": "https://ns-mlflow.example.com",
					"timeout": "15s",
					"settings": {"authType": "kubernetes", "workspacesEnabled": true}
				}`,
			},
		},
	)
	kubeClients := &fakeKubeClientProvider{clientSet: clientSet}

	resolved, err := ResolveMLflowRequestConfig(context.Background(), kubeClients, "ns-only")
	require.NoError(t, err)
	require.NotNil(t, resolved, "namespace-only config should enable MLflow")
	assert.Equal(t, "https://ns-mlflow.example.com", resolved.Config.Endpoint)
	assert.Equal(t, "15s", resolved.Config.Timeout)
	require.NotNil(t, resolved.Settings)
	assert.Equal(t, "kubernetes", resolved.Settings.AuthType)
	require.NotNil(t, resolved.Settings.WorkspacesEnabled)
	assert.True(t, *resolved.Settings.WorkspacesEnabled)
}

// TestResolveMLflowRequestConfig_NeitherGlobalNorNamespace verifies that MLflow
// is disabled (nil return) when both global and namespace configs are absent.
func TestResolveMLflowRequestConfig_NeitherGlobalNorNamespace(t *testing.T) {
	originalPlugins := viper.Get("plugins")
	viper.Set("plugins", nil)
	t.Cleanup(func() {
		viper.Set("plugins", originalPlugins)
	})

	clientSet := k8sfake.NewSimpleClientset() // no ConfigMap
	kubeClients := &fakeKubeClientProvider{clientSet: clientSet}

	resolved, err := ResolveMLflowRequestConfig(context.Background(), kubeClients, "empty-ns")
	require.NoError(t, err)
	assert.Nil(t, resolved, "MLflow should be disabled when neither global nor namespace config exists")
}

func TestResolveMLflowCredentials_EmptySAToken(t *testing.T) {
	// Write only whitespace — empty after TrimSpace.
	setupSATokenFile(t, "   \n\t  \n")

	settings := &MLflowPluginSettings{AuthType: commonmlflow.DefaultAuthType}
	_, err := ResolveMLflowCredentials(context.Background(), nil, "ns1", settings)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "service account token is empty")
}

func TestBuildMLflowRequestContext_InvalidEndpoint(t *testing.T) {
	requestCfg := &ResolvedConfig{
		Config: &PluginConfig{
			Endpoint: "not-a-valid-url",
			Timeout:  "10s",
		},
		Settings: &MLflowPluginSettings{AuthType: commonmlflow.DefaultAuthType},
	}
	ctx, err := BuildMLflowRequestContext(context.Background(), nil, "ns1", requestCfg)
	require.Error(t, err)
	assert.Nil(t, ctx)
	assert.Contains(t, err.Error(), "invalid plugins.mlflow endpoint")
}

func TestBuildMLflowRequestContext_ZeroTimeout(t *testing.T) {
	setupSATokenFile(t, "valid-token")

	requestCfg := &ResolvedConfig{
		Config: &PluginConfig{
			Endpoint: "https://mlflow.example.com",
			Timeout:  "0s",
		},
		Settings: &MLflowPluginSettings{AuthType: commonmlflow.DefaultAuthType},
	}
	ctx, err := BuildMLflowRequestContext(context.Background(), nil, "ns1", requestCfg)
	require.Error(t, err)
	assert.Nil(t, ctx)
	assert.Contains(t, err.Error(), "timeout must be > 0")
}

func TestInjectMLflowRuntimeEnv(t *testing.T) {
	workflow := util.NewWorkflow(&workflowapi.Workflow{
		Spec: workflowapi.WorkflowSpec{
			Templates: []workflowapi.Template{
				{
					Name:      "system-dag-driver",
					Container: &corev1.Container{Args: []string{"--type", "DAG"}},
				},
				{
					Name:      "system-container-impl",
					Container: &corev1.Container{},
					InitContainers: []workflowapi.UserContainer{
						{Container: corev1.Container{Name: "kfp-launcher", Args: []string{"--copy", "/kfp-launcher/launch"}}},
					},
				},
			},
		},
	})

	env := map[string]string{
		commonmlflow.EnvMLflowConfig: `{"endpoint":"https://mlflow.example.com","parentRunId":"abc"}`,
	}
	err := InjectMLflowRuntimeEnv(workflow, env, true)
	require.NoError(t, err)

	// Verify env var is injected on all templates.
	driverEnv := workflow.Spec.Templates[0].Container.Env
	assert.Contains(t, driverEnv, corev1.EnvVar{Name: commonmlflow.EnvMLflowConfig, Value: env[commonmlflow.EnvMLflowConfig]})

	launcherEnv := workflow.Spec.Templates[1].Container.Env
	assert.Contains(t, launcherEnv, corev1.EnvVar{Name: commonmlflow.EnvMLflowConfig, Value: env[commonmlflow.EnvMLflowConfig]})

	initEnv := workflow.Spec.Templates[1].InitContainers[0].Env
	assert.Contains(t, initEnv, corev1.EnvVar{Name: commonmlflow.EnvMLflowConfig, Value: env[commonmlflow.EnvMLflowConfig]})

	// Verify --mlflow_enabled flag is set on driver and launcher init container.
	assert.Contains(t, workflow.Spec.Templates[0].Container.Args, "--mlflow_enabled")
	assert.Contains(t, workflow.Spec.Templates[1].InitContainers[0].Args, "--mlflow_enabled")
}

func TestInjectMLflowRuntimeEnv_NilSpec(t *testing.T) {
	err := InjectMLflowRuntimeEnv(nil, map[string]string{"key": "val"}, true)
	require.NoError(t, err, "nil spec should be a no-op")
}

func TestInjectMLflowRuntimeEnv_EmptyEnv(t *testing.T) {
	workflow := util.NewWorkflow(&workflowapi.Workflow{})
	err := InjectMLflowRuntimeEnv(workflow, map[string]string{}, true)
	require.NoError(t, err, "empty env should be a no-op")
}

// newTestMLflowRequestContext creates a fake Kubernetes clientset with a bearer
// credential Secret and returns a fully wired *RequestContext pointing at serverURL.
func newTestMLflowRequestContext(t *testing.T, serverURL string) *RequestContext {
	t.Helper()
	clientSet := k8sfake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: v1.ObjectMeta{
				Name:      "mlflow-creds",
				Namespace: "ns1",
			},
			Data: map[string][]byte{
				"token": []byte("bearer-secret"),
			},
		},
	)
	enabled := true
	requestCfg := &ResolvedConfig{
		Config: &PluginConfig{
			Endpoint: serverURL,
			Timeout:  "10s",
		},
		Settings: &MLflowPluginSettings{
			AuthType: commonmlflow.AuthTypeBearer,
			CredentialSecretRef: &CredentialSecretRef{
				Name:     "mlflow-creds",
				TokenKey: "token",
			},
			WorkspacesEnabled: &enabled,
		},
	}
	ctx, err := BuildMLflowRequestContext(context.Background(), clientSet, "ns1", requestCfg)
	require.NoError(t, err)
	require.NotNil(t, ctx)
	return ctx
}
