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
	"fmt"
	"strings"

	apiv2beta1 "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	commonmlflow "github.com/kubeflow/pipelines/backend/src/common/mlflow"
	"k8s.io/client-go/kubernetes"
)

// PluginHandler encapsulates the run-level plugin lifecycle hooks for MLflow.
type PluginHandler interface {
	OnRunStart(ctx context.Context, run *apiv2beta1.Run, config *PluginConfig) (*apiv2beta1.PluginOutput, error)
	OnRunEnd(ctx context.Context, run *apiv2beta1.Run, config *PluginConfig) error
}

// HandlerDeps bundles the external dependencies required by Handler.
type HandlerDeps struct {
	KubeClients KubeClientProvider
}

// Handler implements PluginHandler for the MLflow integration.
type Handler struct {
	deps      HandlerDeps
	input     *MLflowPluginInput
	namespace string

	// RunStartEnv is populated by OnRunStart with the single
	// KFP_MLFLOW_CONFIG env var for the driver and launcher.
	RunStartEnv map[string]string

	// MLflowEnabled is set to true by OnRunStart when MLflow
	// integration is active.
	MLflowEnabled bool
}

// NewHandler creates a new MLflow plugin handler with the given dependencies,
// plugin input, and namespace.
func NewHandler(deps HandlerDeps, input *MLflowPluginInput, namespace string) *Handler {
	return &Handler{
		deps:      deps,
		input:     input,
		namespace: namespace,
	}
}

func (h *Handler) clientSet() kubernetes.Interface {
	if h.deps.KubeClients == nil {
		return nil
	}
	return h.deps.KubeClients.GetClientSet()
}

// OnRunStart creates the MLflow experiment and parent run, tags it with KFP
// metadata, and populates RunStartEnv with tracking env vars.
func (h *Handler) OnRunStart(ctx context.Context, run *apiv2beta1.Run, config *PluginConfig) (*apiv2beta1.PluginOutput, error) {
	if h == nil || run == nil || h.input == nil || h.input.Disabled {
		return nil, nil
	}
	if config == nil {
		return nil, nil
	}

	experimentID, experimentName := SelectMLflowExperiment(h.input)

	settings, err := ParseMLflowPluginSettings(config.Settings)
	if err != nil {
		return FailedPluginOutput(experimentID, experimentName, "", "", fmt.Sprintf("failed to parse MLflow settings: %v", err)), err
	}

	resolvedCfg := &ResolvedConfig{Config: config, Settings: settings}
	mlflowRequestCtx, err := BuildMLflowRequestContext(ctx, h.clientSet(), h.namespace, resolvedCfg)
	if err != nil {
		return FailedPluginOutput(experimentID, experimentName, "", "", fmt.Sprintf("failed to build MLflow request context: %v", err)), err
	}
	if mlflowRequestCtx == nil {
		return nil, nil
	}

	var description *string
	if settings.ExperimentDescription != nil {
		description = settings.ExperimentDescription
	}
	mlflowExperiment, err := EnsureExperimentExists(
		ctx,
		mlflowRequestCtx,
		experimentID,
		experimentName,
		ResolveExperimentDescription(description),
	)
	if err != nil {
		return FailedPluginOutput(experimentID, experimentName, "", "", err.Error()), err
	}

	parentRunID, err := mlflowRequestCtx.Client.CreateRun(ctx, mlflowExperiment.ID, run.GetDisplayName(), nil)
	if err != nil {
		return FailedPluginOutput(mlflowExperiment.ID, mlflowExperiment.Name, "", "", err.Error()), err
	}

	authType := commonmlflow.DefaultAuthType
	if settings.AuthType != "" {
		authType = settings.AuthType
	}
	insecureSkipVerify := false
	if config.TLS != nil {
		insecureSkipVerify = config.TLS.InsecureSkipVerify
	}
	mlflowRuntimeConfig := commonmlflow.MLflowRuntimeConfig{
		Endpoint:           mlflowRequestCtx.BaseURL.String(),
		Workspace:          h.namespace,
		ParentRunID:        parentRunID,
		ExperimentID:       mlflowExperiment.ID,
		AuthType:           authType,
		Timeout:            config.Timeout,
		InsecureSkipVerify: insecureSkipVerify,
	}
	mlflowConfigJSON, err := json.Marshal(mlflowRuntimeConfig)
	if err != nil {
		return FailedPluginOutput(mlflowExperiment.ID, mlflowExperiment.Name, parentRunID, "", fmt.Sprintf("failed to marshal MLflow runtime config: %v", err)), err
	}
	// All MLflow settings resolved successfully; enable MLflow for this run.
	h.RunStartEnv = map[string]string{
		commonmlflow.EnvMLflowConfig: string(mlflowConfigJSON),
	}
	h.MLflowEnabled = true

	if err := TagRunWithKFPMetadata(ctx, mlflowRequestCtx, parentRunID, run); err != nil {
		return FailedPluginOutput(mlflowExperiment.ID, mlflowExperiment.Name, parentRunID, "", err.Error()), err
	}

	runURL := BuildRunURL(mlflowRequestCtx, mlflowExperiment.ID, parentRunID)
	return SuccessfulPluginOutput(mlflowExperiment.ID, mlflowExperiment.Name, parentRunID, runURL), nil
}

// OnRunEnd marks the MLflow parent run and any active nested runs as
// complete/failed when the KFP run reaches a terminal state.
func (h *Handler) OnRunEnd(ctx context.Context, run *apiv2beta1.Run, config *PluginConfig) error {
	if h == nil || run == nil {
		return nil
	}
	return h.syncOnRunTerminal(ctx, run, config)
}

// syncOnRunTerminal marks the MLflow parent and nested runs as complete/failed.
func (h *Handler) syncOnRunTerminal(ctx context.Context, run *apiv2beta1.Run, config *PluginConfig) error {
	endTimeMs := int64(0)
	endTimeRef := (*int64)(nil)
	if run.GetFinishedAt() != nil {
		endTimeMs = run.GetFinishedAt().AsTime().UnixMilli()
		endTimeRef = &endTimeMs
	}
	terminalStatus := commonmlflow.ToMLflowTerminalStatus(run.GetState().String())
	h.syncMLflowRuns(ctx, run, config, RunSyncModeTerminal, terminalStatus, endTimeRef, "terminal")
	return nil
}

// HandleRetry reopens the MLflow parent run and any failed/killed nested runs.
func (h *Handler) HandleRetry(ctx context.Context, run *apiv2beta1.Run, config *PluginConfig) {
	h.syncMLflowRuns(ctx, run, config, RunSyncModeRetry, "", nil, "retry")
}

// syncMLflowRuns resolves the MLflow request context, syncs the parent and nested runs, and
// updates the plugin output state.
func (h *Handler) syncMLflowRuns(ctx context.Context, run *apiv2beta1.Run, config *PluginConfig, mode RunSyncMode, terminalStatus string, endTimeRef *int64, label string) {
	pluginOutput := run.GetPluginsOutput()[PluginName]
	if pluginOutput == nil {
		return
	}

	parentRunID := GetParentRunID(pluginOutput)
	experimentID := GetStringEntry(pluginOutput, EntryExperimentID)
	if parentRunID == "" {
		SetPluginOutputState(pluginOutput, apiv2beta1.PluginState_PLUGIN_FAILED, fmt.Sprintf("MLflow %s sync skipped: missing parent root_run_id in plugins_output.mlflow", label))
		return
	}

	if config == nil {
		SetPluginOutputState(pluginOutput, apiv2beta1.PluginState_PLUGIN_FAILED, fmt.Sprintf("MLflow %s sync failed: config unavailable", label))
		return
	}

	settings, err := ParseMLflowPluginSettings(config.Settings)
	if err != nil {
		SetPluginOutputState(pluginOutput, apiv2beta1.PluginState_PLUGIN_FAILED, fmt.Sprintf("MLflow %s sync failed: %v", label, err))
		return
	}

	resolvedCfg := &ResolvedConfig{Config: config, Settings: settings}
	mlflowRequestCtx, err := BuildMLflowRequestContext(ctx, h.clientSet(), h.namespace, resolvedCfg)
	if err != nil || mlflowRequestCtx == nil || mlflowRequestCtx.Client == nil {
		message := fmt.Sprintf("MLflow %s sync failed: unable to build MLflow request context", label)
		if err != nil {
			message = fmt.Sprintf("MLflow %s sync failed: %v", label, err)
		}
		SetPluginOutputState(pluginOutput, apiv2beta1.PluginState_PLUGIN_FAILED, message)
		return
	}

	syncErrors := SyncParentAndNestedRuns(ctx, mlflowRequestCtx, parentRunID, experimentID, mode, terminalStatus, endTimeRef)
	if len(syncErrors) > 0 {
		SetPluginOutputState(pluginOutput, apiv2beta1.PluginState_PLUGIN_FAILED, strings.Join(syncErrors, "; "))
	} else {
		SetPluginOutputState(pluginOutput, apiv2beta1.PluginState_PLUGIN_SUCCEEDED, "")
	}
}
