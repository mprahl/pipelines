package util

import (
	"context"
	"encoding/json"
	"fmt"

	commonmlflow "github.com/kubeflow/pipelines/backend/src/common/mlflow"
	"github.com/kubeflow/pipelines/backend/src/v2/common/mlflow"
	"github.com/kubeflow/pipelines/backend/src/v2/metadata"
)

type MLflowRuntimeConfig = commonmlflow.MLflowRuntimeConfig

func ApplyMLflowOnTaskStart(ctx context.Context, kfpCfgJson string, taskName string) (string, error) {
	var cfg MLflowRuntimeConfig
	err := json.Unmarshal([]byte(kfpCfgJson), &cfg)
	if err != nil {
		return "", err
	}

	handler := mlflow.NewHandler()
	taskInfo, err := getTaskInfoFromRuntimeConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("failed to get task taskInfo from runtime config: %v", err)
	}
	pluginCfg := getPluginConfigFromRuntimeConfig(cfg)
	nestedRunID, taskStartErr := handler.OnTaskStart(ctx, *taskInfo, pluginCfg)

	if taskStartErr != nil {
		return "", fmt.Errorf("MLflow OnTaskStart failed for task %q (task creation will continue): %v", taskName, taskStartErr)
	}
	if nestedRunID == "" {
		return "", fmt.Errorf("MLflow OnTaskStart returned empty run ID for task %q (run creation will continue)", taskName)
	}
	return nestedRunID, nil
}

func ApplyMLflowOnTaskEnd(ctx context.Context, runID string, runtimeCfgJson string, execution *metadata.Execution) error {
	handler := mlflow.NewHandler()

	var runtimeCfg MLflowRuntimeConfig
	err := json.Unmarshal([]byte(runtimeCfgJson), &runtimeCfg)
	if err != nil {
		return err
	}

	pluginCfg := getPluginConfigFromRuntimeConfig(runtimeCfg)
	taskInfo, err := getTaskInfoFromRuntimeConfig(runtimeCfg)
	if err != nil {
		return fmt.Errorf("Failed to get task info from runtime config: %v", err)
	}
	taskInfo.RunID = runID
	taskInfo.RunEndTime = execution.GetExecution().GetLastUpdateTimeSinceEpoch()
	taskInfo.RunStatus = execution.GetExecution().LastKnownState.String()

	var metrics []commonmlflow.Metric
	exec := execution.GetExecution()

	metrics = append(metrics,
		commonmlflow.Metric{
			Key:   "CreateTimeSinceEpoch",
			Value: float64(*exec.CreateTimeSinceEpoch),
		})
	metrics = append(metrics,
		commonmlflow.Metric{
			Key:   "LastUpdateTimeSinceEpoch",
			Value: float64(*exec.LastUpdateTimeSinceEpoch),
		})

	var params []commonmlflow.Param
	inputParams, _, err := execution.GetParameters()
	if err != nil {
		return err
	}
	for key, value := range inputParams {
		params = append(params, commonmlflow.Param{
			Key:   key,
			Value: value.GetStringValue(),
		})
	}

	return handler.OnTaskEnd(ctx, *taskInfo, metrics, params, pluginCfg)
}

func getTaskInfoFromRuntimeConfig(runtimeCfg MLflowRuntimeConfig) (*mlflow.TaskInfo, error) {
	if runtimeCfg.ParentRunID == "" {
		return nil, fmt.Errorf("ParentRunID is required to create MLflow task")
	}
	if runtimeCfg.ExperimentID == "" {
		return nil, fmt.Errorf("ExperimentID is required to create MLflow task")
	}
	if runtimeCfg.AuthType == "" {
		return nil, fmt.Errorf("AuthType is required to create MLflow task")
	}
	return &mlflow.TaskInfo{
		Workspace:         runtimeCfg.Workspace,
		WorkspacesEnabled: runtimeCfg.Workspace != "",
		ParentRunID:       runtimeCfg.ParentRunID,
		ExperimentID:      runtimeCfg.ExperimentID,
		AuthType:          runtimeCfg.AuthType,
	}, nil
}

func getPluginConfigFromRuntimeConfig(runtimeCfg MLflowRuntimeConfig) *commonmlflow.PluginConfig {
	tlsCfg := commonmlflow.TLSConfig{
		InsecureSkipVerify: runtimeCfg.InsecureSkipVerify,
	}
	return &commonmlflow.PluginConfig{
		Endpoint: runtimeCfg.Endpoint,
		Timeout:  runtimeCfg.Timeout,
		TLS:      &tlsCfg,
	}
}
