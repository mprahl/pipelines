package mlflow

import (
	"context"
	"fmt"

	commonmlflow "github.com/kubeflow/pipelines/backend/src/common/mlflow"
)

// Handler implements PluginHandler for the MLflow integration.
type Handler struct {
}

// NewHandler creates a new MLflow plugin handler with the given dependencies
// and plugin input.
func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) OnTaskStart(ctx context.Context, taskInfo TaskInfo, config *commonmlflow.PluginConfig) (string, error) {
	if h == nil || config == nil {
		return "", nil
	}
	if taskInfo.ParentRunID == "" || taskInfo.ExperimentID == "" {
		return "", fmt.Errorf("ParentRunID and ExperimentID are required to create nested MLflow run")
	}
	mlflowRequestCtx, err := BuildTaskRequestContext(taskInfo, *config)
	if err != nil {
		return "", err
	}
	if mlflowRequestCtx == nil {
		return "", fmt.Errorf("failed to build MLflow request context: %v", err)
	}
	nestedRunID, err := CreateNestedRun(ctx, mlflowRequestCtx, taskInfo.ExperimentID, taskInfo.ParentRunID)
	if err != nil {
		return "", err
	}

	return nestedRunID, nil
}

func (h *Handler) OnTaskEnd(ctx context.Context, taskInfo TaskInfo, metrics []commonmlflow.Metric, params []commonmlflow.Param, config *commonmlflow.PluginConfig) error {

	if taskInfo.RunID == "" || taskInfo.ExperimentID == "" {
		return fmt.Errorf("RunID and ExperimentID are both required to update MLflow run")
	}

	mlflowRequestCtx, err := BuildTaskRequestContext(taskInfo, *config)
	if err != nil {
		return fmt.Errorf("failed to build MLflow request context: %v", err)
	}

	endTime := taskInfo.RunEndTime

	err = UpdateRun(ctx, mlflowRequestCtx, taskInfo.RunID, taskInfo.RunStatus, endTime)
	if err != nil {
		return fmt.Errorf("failed to update MLflow run: %v", err)
	}

	err = LogBatch(ctx, mlflowRequestCtx, taskInfo.RunID, metrics, params, []commonmlflow.Tag{})
	if err != nil {
		return fmt.Errorf("failed to log metrics and params to MLflow: %v", err)
	}

	return nil
}
