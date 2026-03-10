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
	"strings"
	"testing"

	apiv2beta1 "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	commonmlflow "github.com/kubeflow/pipelines/backend/src/common/mlflow"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

// ---- Helpers ----`

func setupSAToken(t *testing.T) func() {
	t.Helper()
	tokenFile, err := os.CreateTemp(t.TempDir(), "sa-token-*")
	require.NoError(t, err)
	_, err = tokenFile.WriteString("test-sa-token\n")
	require.NoError(t, err)
	require.NoError(t, tokenFile.Close())
	orig := commonmlflow.ServiceAccountTokenPath
	commonmlflow.ServiceAccountTokenPath = tokenFile.Name()
	return func() { commonmlflow.ServiceAccountTokenPath = orig }
}

func testPluginConfig(endpoint string) *PluginConfig {
	return &PluginConfig{
		Endpoint: endpoint,
		Timeout:  "10s",
		Settings: []byte(`{"authType":"kubernetes","workspacesEnabled":false}`),
	}
}

func testRun(id, displayName string) *apiv2beta1.Run {
	return &apiv2beta1.Run{
		RunId:       id,
		DisplayName: displayName,
	}
}

func testRunWithPipeline(id, displayName, pipelineID, versionID string) *apiv2beta1.Run {
	r := testRun(id, displayName)
	if pipelineID != "" || versionID != "" {
		r.PipelineSource = &apiv2beta1.Run_PipelineVersionReference{
			PipelineVersionReference: &apiv2beta1.PipelineVersionReference{
				PipelineId:        pipelineID,
				PipelineVersionId: versionID,
			},
		}
	}
	return r
}

func testRunWithPluginOutput(id string, pluginOutput *apiv2beta1.PluginOutput) *apiv2beta1.Run {
	r := &apiv2beta1.Run{RunId: id}
	if pluginOutput != nil {
		r.PluginsOutput = map[string]*apiv2beta1.PluginOutput{
			PluginName: pluginOutput,
		}
	}
	return r
}

// ---- OnRunStart tests ----

func TestOnRunStart_NilConfig_ReturnsNil(t *testing.T) {
	handler := NewHandler(HandlerDeps{}, &MLflowPluginInput{ExperimentName: "Default"}, "ns1")
	output, err := handler.OnRunStart(context.Background(), testRun("r1", "run-1"), nil)
	require.NoError(t, err)
	assert.Nil(t, output)
	assert.Empty(t, handler.RunStartEnv)
}

func TestOnRunStart_Disabled_ReturnsNil(t *testing.T) {
	handler := NewHandler(HandlerDeps{}, &MLflowPluginInput{Disabled: true}, "ns1")
	output, err := handler.OnRunStart(context.Background(), testRun("r1", "run-1"), testPluginConfig("http://localhost"))
	require.NoError(t, err)
	assert.Nil(t, output)
}

func TestOnRunStart_NilInput_ReturnsNil(t *testing.T) {
	handler := NewHandler(HandlerDeps{}, nil, "ns1")
	output, err := handler.OnRunStart(context.Background(), testRun("r1", "run-1"), testPluginConfig("http://localhost"))
	require.NoError(t, err)
	assert.Nil(t, output)
}

func TestOnRunStart_Success(t *testing.T) {
	cleanup := setupSAToken(t)
	defer cleanup()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/2.0/mlflow/experiments/get-by-name":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"experiment":{"experiment_id":"exp-42","name":"Default"}}`))
		case "/api/2.0/mlflow/runs/create":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"run":{"info":{"run_id":"mlflow-run-1"}}}`))
		case "/api/2.0/mlflow/runs/set-tag":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	viper.Set(common.MultiUserMode, false)
	t.Cleanup(func() { viper.Set(common.MultiUserMode, nil) })

	handler := NewHandler(HandlerDeps{}, &MLflowPluginInput{ExperimentName: "Default"}, "ns1")

	run := testRun("kfp-run-1", "my-run")
	output, err := handler.OnRunStart(context.Background(), run, testPluginConfig(server.URL))
	require.NoError(t, err)
	require.NotNil(t, output)

	assert.Equal(t, apiv2beta1.PluginState_PLUGIN_SUCCEEDED, output.State)
	assert.Contains(t, output.Entries, EntryExperimentID)
	assert.Equal(t, "exp-42", output.Entries[EntryExperimentID].Value.GetStringValue())
	assert.Contains(t, output.Entries, EntryRootRunID)
	assert.Equal(t, "mlflow-run-1", output.Entries[EntryRootRunID].Value.GetStringValue())

	// Verify RunStartEnv contains single KFP_MLFLOW_CONFIG JSON env var
	require.NotEmpty(t, handler.RunStartEnv)
	assert.Contains(t, handler.RunStartEnv, commonmlflow.EnvMLflowConfig)
	assert.True(t, handler.MLflowEnabled)

	var rtCfg commonmlflow.MLflowRuntimeConfig
	require.NoError(t, json.Unmarshal([]byte(handler.RunStartEnv[commonmlflow.EnvMLflowConfig]), &rtCfg))
	assert.Contains(t, rtCfg.Endpoint, server.URL)
	assert.Equal(t, "ns1", rtCfg.Workspace)
	assert.Equal(t, "mlflow-run-1", rtCfg.ParentRunID)
	assert.Equal(t, "exp-42", rtCfg.ExperimentID)
	assert.Equal(t, "kubernetes", rtCfg.AuthType)
}

func TestOnRunStart_MLflowFailure_ReturnsFailedOutput(t *testing.T) {
	cleanup := setupSAToken(t)
	defer cleanup()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error_code":"INTERNAL_ERROR","message":"server down"}`))
	}))
	defer server.Close()

	viper.Set(common.MultiUserMode, false)
	t.Cleanup(func() { viper.Set(common.MultiUserMode, nil) })

	handler := NewHandler(HandlerDeps{}, &MLflowPluginInput{ExperimentName: "Default"}, "ns1")

	run := testRun("kfp-run-2", "run-2")
	output, err := handler.OnRunStart(context.Background(), run, testPluginConfig(server.URL))
	require.Error(t, err)
	require.NotNil(t, output)
	assert.Equal(t, apiv2beta1.PluginState_PLUGIN_FAILED, output.State)
	assert.NotEmpty(t, output.StateMessage)
}

// ---- OnRunEnd / syncOnRunTerminal tests ----

func TestOnRunEnd_NilRun_ReturnsNil(t *testing.T) {
	handler := NewHandler(HandlerDeps{}, nil, "ns1")
	err := handler.OnRunEnd(context.Background(), nil, testPluginConfig("http://localhost"))
	require.NoError(t, err)
}

func TestOnRunEnd_NoPluginOutput_ReturnsNil(t *testing.T) {
	handler := NewHandler(HandlerDeps{}, nil, "ns1")
	run := testRun("r1", "run-1")
	err := handler.OnRunEnd(context.Background(), run, testPluginConfig("http://localhost"))
	require.NoError(t, err)
}

func TestOnRunEnd_MissingRootRunID_SetsFailedState(t *testing.T) {
	handler := NewHandler(HandlerDeps{}, nil, "ns1")

	// Build a run with plugin output that has no root_run_id
	pluginOutput := SuccessfulPluginOutput("42", "Default", "", "")
	run := testRunWithPluginOutput("r-missing-root", pluginOutput)

	err := handler.OnRunEnd(context.Background(), run, testPluginConfig("http://localhost"))
	require.NoError(t, err)

	// Verify the plugin output was updated in place
	result := run.PluginsOutput[PluginName]
	require.NotNil(t, result)
	assert.Equal(t, apiv2beta1.PluginState_PLUGIN_FAILED, result.State)
	assert.Contains(t, result.StateMessage, "missing parent root_run_id")
}

func TestOnRunEnd_NilConfig_SetsFailedState(t *testing.T) {
	handler := NewHandler(HandlerDeps{}, nil, "ns1")

	pluginOutput := SuccessfulPluginOutput("42", "Default", "parent-1", "")
	run := testRunWithPluginOutput("r-nil-config", pluginOutput)

	err := handler.OnRunEnd(context.Background(), run, nil)
	require.NoError(t, err)

	result := run.PluginsOutput[PluginName]
	require.NotNil(t, result)
	assert.Equal(t, apiv2beta1.PluginState_PLUGIN_FAILED, result.State)
	assert.Contains(t, result.StateMessage, "config unavailable")
}

func TestOnRunEnd_Success(t *testing.T) {
	cleanup := setupSAToken(t)
	defer cleanup()

	var updateCalls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/2.0/mlflow/runs/update":
			body, _ := io.ReadAll(r.Body)
			updateCalls = append(updateCalls, string(body))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case "/api/2.0/mlflow/runs/search":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"runs":[]}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	handler := NewHandler(HandlerDeps{}, nil, "ns1")

	pluginOutput := SuccessfulPluginOutput("exp-1", "Default", "mlflow-parent-1", "")
	run := testRunWithPluginOutput("r-end-1", pluginOutput)
	run.State = apiv2beta1.RuntimeState_SUCCEEDED

	err := handler.OnRunEnd(context.Background(), run, testPluginConfig(server.URL))
	require.NoError(t, err)

	// Parent run should have been updated
	require.NotEmpty(t, updateCalls)
	assert.Contains(t, updateCalls[0], "mlflow-parent-1")
	assert.Contains(t, updateCalls[0], "FINISHED") // SUCCEEDED maps to FINISHED

	// Plugin output should be updated in place
	result := run.PluginsOutput[PluginName]
	require.NotNil(t, result)
	assert.Equal(t, apiv2beta1.PluginState_PLUGIN_SUCCEEDED, result.State)
}

// ---- HandleRetry tests ----

func TestHandleRetry_NoPluginOutput_NoOp(t *testing.T) {
	handler := NewHandler(HandlerDeps{}, nil, "ns1")
	run := testRun("r-retry-noop", "run-noop")

	handler.HandleRetry(context.Background(), run, testPluginConfig("http://localhost"))
	// No plugin output → nothing to do
	assert.Nil(t, run.PluginsOutput)
}

func TestHandleRetry_MissingRootRunID_SetsFailedState(t *testing.T) {
	handler := NewHandler(HandlerDeps{}, nil, "ns1")

	pluginOutput := SuccessfulPluginOutput("42", "Default", "", "")
	run := testRunWithPluginOutput("r-retry-no-root", pluginOutput)

	handler.HandleRetry(context.Background(), run, testPluginConfig("http://localhost"))

	result := run.PluginsOutput[PluginName]
	require.NotNil(t, result)
	assert.Equal(t, apiv2beta1.PluginState_PLUGIN_FAILED, result.State)
	assert.Contains(t, result.StateMessage, "missing parent root_run_id")
}

func TestHandleRetry_NilConfig_SetsFailedState(t *testing.T) {
	handler := NewHandler(HandlerDeps{}, nil, "ns1")

	pluginOutput := SuccessfulPluginOutput("42", "Default", "parent-1", "")
	run := testRunWithPluginOutput("r-retry-nil-config", pluginOutput)

	handler.HandleRetry(context.Background(), run, nil)

	result := run.PluginsOutput[PluginName]
	require.NotNil(t, result)
	assert.Equal(t, apiv2beta1.PluginState_PLUGIN_FAILED, result.State)
	assert.Contains(t, result.StateMessage, "config unavailable")
}

func TestHandleRetry_Success(t *testing.T) {
	cleanup := setupSAToken(t)
	defer cleanup()

	var updatePayloads []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/2.0/mlflow/runs/update":
			body, _ := io.ReadAll(r.Body)
			updatePayloads = append(updatePayloads, string(body))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case "/api/2.0/mlflow/runs/search":
			// Return one failed nested run
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"runs":[{"info":{"run_id":"nested-1","status":"FAILED"}}]}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	handler := NewHandler(HandlerDeps{}, nil, "ns1")

	pluginOutput := FailedPluginOutput("exp-1", "Default", "parent-1", "", "previous failure")
	run := testRunWithPluginOutput("r-retry-ok", pluginOutput)

	handler.HandleRetry(context.Background(), run, testPluginConfig(server.URL))

	// Parent reopened + nested-1 reopened = 2 update calls
	require.Len(t, updatePayloads, 2)
	assert.Contains(t, updatePayloads[0], "parent-1")
	assert.Contains(t, updatePayloads[0], "RUNNING")
	assert.Contains(t, updatePayloads[1], "nested-1")
	assert.Contains(t, updatePayloads[1], "RUNNING")

	// Plugin output updated in place
	result := run.PluginsOutput[PluginName]
	require.NotNil(t, result)
	assert.Equal(t, apiv2beta1.PluginState_PLUGIN_SUCCEEDED, result.State)
}

// ---- BuildKFPRunURL tests ----

func TestBuildKFPRunURL(t *testing.T) {
	tests := []struct {
		name    string
		runID   string
		wantURL string
	}{
		{
			name:    "empty runID returns empty",
			runID:   "",
			wantURL: "",
		},
		{
			name:    "returns relative path",
			runID:   "abc",
			wantURL: "/#/runs/details/abc",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildKFPRunURL(tt.runID)
			assert.Equal(t, tt.wantURL, got)
		})
	}
}

// ---- ModelToPluginRun tests ----

func TestModelToPluginRun_NilModel(t *testing.T) {
	assert.Nil(t, ModelToPluginRun(nil))
}

func TestModelToPluginRun_BasicFields(t *testing.T) {
	m := &model.Run{
		UUID:        "run-123",
		DisplayName: "My Run",
	}
	m.PipelineSpec.PipelineId = "pipe-1"
	m.PipelineSpec.PipelineVersionId = "ver-1"
	m.RunDetails.State = "SUCCEEDED"
	m.RunDetails.FinishedAtInSec = 1700000000

	apiRun := ModelToPluginRun(m)
	require.NotNil(t, apiRun)
	assert.Equal(t, "run-123", apiRun.GetRunId())
	assert.Equal(t, "My Run", apiRun.GetDisplayName())
	assert.Equal(t, apiv2beta1.RuntimeState_SUCCEEDED, apiRun.GetState())
	require.NotNil(t, apiRun.GetFinishedAt())
	assert.Equal(t, int64(1700000000), apiRun.GetFinishedAt().AsTime().Unix())
	ref := apiRun.GetPipelineVersionReference()
	require.NotNil(t, ref)
	assert.Equal(t, "pipe-1", ref.PipelineId)
	assert.Equal(t, "ver-1", ref.PipelineVersionId)
}

// ---- SyncPluginOutputToModel tests ----

func TestSyncPluginOutputToModel_MergesWithExisting(t *testing.T) {
	existing := `{"other_plugin":{"state":"PLUGIN_SUCCEEDED"}}`
	lt := model.LargeText(existing)
	modelRun := &model.Run{
		UUID: "r1",
	}
	modelRun.RunDetails.PluginsOutputString = &lt

	apiRun := &apiv2beta1.Run{
		RunId: "r1",
		PluginsOutput: map[string]*apiv2beta1.PluginOutput{
			"mlflow": SuccessfulPluginOutput("exp-1", "Default", "parent-1", ""),
		},
	}

	err := SyncPluginOutputToModel(apiRun, modelRun)
	require.NoError(t, err)
	require.NotNil(t, modelRun.PluginsOutputString)
	// Both plugins should be present
	assert.Contains(t, string(*modelRun.PluginsOutputString), "other_plugin")
	assert.Contains(t, string(*modelRun.PluginsOutputString), "mlflow")
}

// ---- SyncParentAndNestedRuns pagination test ----

func TestSyncParentAndNestedRuns_Pagination(t *testing.T) {
	cleanup := setupSAToken(t)
	defer cleanup()

	clientSet := k8sfake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: v1.ObjectMeta{Name: "mlflow-creds", Namespace: "ns1"},
			Data:       map[string][]byte{"token": []byte("tok")},
		},
	)

	var searchCalls int
	var updateCalls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/2.0/mlflow/runs/update":
			body, _ := io.ReadAll(r.Body)
			updateCalls = append(updateCalls, string(body))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case "/api/2.0/mlflow/runs/search":
			searchCalls++
			if searchCalls == 1 {
				// First page returns one run + next_page_token
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"runs":[{"info":{"run_id":"nested-p1","status":"RUNNING"}}],"next_page_token":"page2"}`))
			} else {
				// Second page returns one run + no token
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"runs":[{"info":{"run_id":"nested-p2","status":"RUNNING"}}]}`))
			}
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	enabled := true
	requestCfg := &ResolvedConfig{
		Config:   &PluginConfig{Endpoint: server.URL, Timeout: "10s"},
		Settings: &MLflowPluginSettings{AuthType: commonmlflow.AuthTypeBearer, CredentialSecretRef: &CredentialSecretRef{Name: "mlflow-creds", TokenKey: "token"}, WorkspacesEnabled: &enabled},
	}
	mlflowCtx, err := BuildMLflowRequestContext(context.Background(), clientSet, "ns1", requestCfg)
	require.NoError(t, err)

	endTime := int64(1700000000000)
	syncErrors := SyncParentAndNestedRuns(context.Background(), mlflowCtx, "parent-1", "exp-1", RunSyncModeTerminal, "FINISHED", &endTime)
	assert.Empty(t, syncErrors)

	// 2 search calls (pagination)
	assert.Equal(t, 2, searchCalls)
	// 1 parent update + 2 nested updates = 3 total
	assert.Len(t, updateCalls, 3)
	// Verify nested runs were updated
	found := strings.Join(updateCalls, " | ")
	assert.Contains(t, found, "nested-p1")
	assert.Contains(t, found, "nested-p2")
}
