package mlflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	commonmlflow "github.com/kubeflow/pipelines/backend/src/common/mlflow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func testTaskInfo(runID string) TaskInfo {
	return TaskInfo{
		RunID:        runID,
		ExperimentID: "exp-1",
		AuthType:     "kubernetes",
		RunEndTime:   10,
	}
}

func testMetrics() []commonmlflow.Metric {
	return []commonmlflow.Metric{
		{
			Key:       "test-metric",
			Value:     0.5,
			Timestamp: time.Now().UnixNano(),
			Step:      1,
		},
	}
}

func testParams() []commonmlflow.Param {
	return []commonmlflow.Param{
		{
			Key:   "test-param",
			Value: "test-value",
		},
	}
}

func TestOnTaskStart_NilPluginConfig_ReturnsEmptyString(t *testing.T) {
	handler := NewHandler()
	nestedRunID, err := handler.OnTaskStart(context.Background(), testTaskInfo("r1"), nil)

	assert.Empty(t, nestedRunID)
	require.NoError(t, err)
}

func TestOnTaskStart_MissingRunID_Failure(t *testing.T) {
	handler := NewHandler()
	nestedRunID, err := handler.OnTaskStart(context.Background(), TaskInfo{ExperimentID: "1", AuthType: "kubernetes"}, testPluginConfig("http://localhost"))
	assert.Empty(t, nestedRunID)

	require.Error(t, err)
	assert.Equal(t, "RunID and ExperimentID are required to create MLflow run", err.Error())
}

func TestOnTaskStart_MissingExperimentID_Failure(t *testing.T) {
	handler := NewHandler()
	nestedRunID, err := handler.OnTaskStart(context.Background(), TaskInfo{RunID: "r1", AuthType: "kubernetes"}, testPluginConfig("http://localhost"))
	assert.Empty(t, nestedRunID)

	require.Error(t, err)
	assert.Equal(t, "RunID and ExperimentID are required to create MLflow run", err.Error())
}

func TestOnTaskStart_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/2.0/mlflow/runs/create":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"run":{"info":{"run_id":"mlflow-run-1"}}}`))
		case "/api/2.0/mlflow/runs/update":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"run":{"info":{"run_id":"mlflow-run-1"}}}`))
		case "/api/2.0/mlflow/runs/log-batch":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	handler := NewHandler()

	taskInfo := testTaskInfo("r1")
	runID, err := handler.OnTaskStart(context.Background(), taskInfo, testPluginConfig(server.URL))
	require.NoError(t, err)
	require.NotEmpty(t, runID)

}

func TestOnTaskStart_MLflowFailure_ReturnsEmptyNestedRunID(t *testing.T) {
	cleanup := setupSAToken(t)
	defer cleanup()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error_code":"INTERNAL_ERROR","message":"server down"}`))
	}))
	defer server.Close()

	handler := NewHandler()
	nestedRunID, err := handler.OnTaskStart(context.Background(), testTaskInfo("r1"), testPluginConfig(server.URL))
	require.Error(t, err)
	require.Empty(t, nestedRunID)
}

func TestOnTaskEnd_MissingRunID_Failure(t *testing.T) {
	handler := NewHandler()
	err := handler.OnTaskEnd(context.Background(), TaskInfo{ExperimentID: "test-experiment", AuthType: "kubernetes"}, testMetrics(), testParams(), testPluginConfig("test-endpoint"))

	require.Error(t, err)
	assert.Equal(t, "RunID and ExperimentID are required to create MLflow run", err.Error())
}

func TestOnTaskEnd_MissingExperimentID_Failure(t *testing.T) {
	handler := NewHandler()
	err := handler.OnTaskEnd(context.Background(), TaskInfo{RunID: "test-run", AuthType: "kubernetes"}, testMetrics(), testParams(), testPluginConfig("test-endpoint"))

	require.Error(t, err)
	assert.Equal(t, "RunID and ExperimentID are required to create MLflow run", err.Error())
}

func TestOnTaskEnd_EmptyMetrics_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/2.0/mlflow/runs/create":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"run":{"info":{"run_id":"mlflow-run-1"}}}`))
		case "/api/2.0/mlflow/runs/update":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"run":{"info":{"run_id":"mlflow-run-1"}}}`))
		case "/api/2.0/mlflow/runs/log-batch":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	handler := NewHandler()
	err := handler.OnTaskEnd(context.Background(), testTaskInfo("run-1"), []commonmlflow.Metric{}, testParams(), testPluginConfig(server.URL))
	require.NoError(t, err)
}

func TestOnTaskEnd_EmptyParams_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/2.0/mlflow/runs/create":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"run":{"info":{"run_id":"mlflow-run-1"}}}`))
		case "/api/2.0/mlflow/runs/update":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"run":{"info":{"run_id":"mlflow-run-1"}}}`))
		case "/api/2.0/mlflow/runs/log-batch":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	handler := NewHandler()
	err := handler.OnTaskEnd(context.Background(), testTaskInfo("run-1"), testMetrics(), []commonmlflow.Param{}, testPluginConfig(server.URL))
	require.NoError(t, err)
}

func TestOnTaskEnd_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/2.0/mlflow/runs/create":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"run":{"info":{"run_id":"mlflow-run-1"}}}`))
		case "/api/2.0/mlflow/runs/update":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"run":{"info":{"run_id":"mlflow-run-1"}}}`))
		case "/api/2.0/mlflow/runs/log-batch":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	handler := NewHandler()

	taskInfo := testTaskInfo("r1")
	err := handler.OnTaskEnd(context.Background(), taskInfo, testMetrics(), testParams(), testPluginConfig(server.URL))
	require.NoError(t, err)
}

func TestOnTaskEnd_EmptyRunID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/2.0/mlflow/runs/create":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"run":{"info":{"run_id":"mlflow-run-1"}}}`))
		case "/api/2.0/mlflow/runs/update":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"run":{"info":{"run_id":"mlflow-run-1"}}}`))
		case "/api/2.0/mlflow/runs/log-batch":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	handler := NewHandler()

	taskInfo := testTaskInfo("")
	err := handler.OnTaskEnd(context.Background(), taskInfo, testMetrics(), testParams(), testPluginConfig(server.URL))
	require.Error(t, err)
	assert.Equal(t, "RunID and ExperimentID are both required to update MLflow run", err.Error())
}
