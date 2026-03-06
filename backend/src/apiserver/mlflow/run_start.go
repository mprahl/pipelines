package mlflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	apiv2beta1 "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	commonmlflow "github.com/kubeflow/pipelines/backend/src/common/mlflow"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	"google.golang.org/protobuf/encoding/protojson"
	structpb "google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	DefaultExperimentDescription = "Created by Kubeflow Pipelines"
	PluginName                   = "mlflow"
	TagKFPRunID                  = "kfp.pipeline_run_id"
	TagKFPRunURL                 = "kfp.pipeline_run_url"
	TagKFPPipelineID             = "kfp.pipeline_id"
	TagKFPPipelineVersionID      = "kfp.pipeline_version_id"
	EntryExperimentName          = "experiment_name"
	EntryExperimentID            = "experiment_id"
	EntryRootRunID               = "root_run_id"
	EntryRunURL                  = "run_url"
)

type RequestContext struct {
	BaseURL           *url.URL
	Client            *commonmlflow.Client
	Workspace         string
	WorkspacesEnabled bool
}

type Experiment struct {
	ID   string
	Name string
}

type RunSyncMode string

const (
	RunSyncModeTerminal RunSyncMode = "terminal"
	RunSyncModeRetry    RunSyncMode = "retry"
)

// EnsureExperimentExists looks up the MLflow experiment by ID or name, and creates it
// if it does not already exist.
func EnsureExperimentExists(ctx context.Context, requestCtx *RequestContext, experimentID, experimentName string, description *string) (*Experiment, error) {
	if requestCtx == nil || requestCtx.Client == nil {
		return nil, util.NewInvalidInputError("MLflow request context is required")
	}
	if experimentID != "" {
		return &Experiment{ID: experimentID, Name: experimentName}, nil
	}
	existing, err := requestCtx.Client.GetExperimentByName(ctx, experimentName)
	if err == nil {
		return &Experiment{ID: existing.ID, Name: existing.Name}, nil
	}
	if !commonmlflow.IsNotFoundError(err) {
		return nil, err
	}
	return CreateExperiment(ctx, requestCtx, experimentName, description)
}

// CreateExperiment creates an MLflow experiment and handles the race condition
// where another request may have created the same experiment concurrently.
func CreateExperiment(ctx context.Context, requestCtx *RequestContext, experimentName string, description *string) (*Experiment, error) {
	createdID, createErr := requestCtx.Client.CreateExperiment(ctx, experimentName, description)
	if createErr == nil {
		return &Experiment{ID: createdID, Name: experimentName}, nil
	}
	if commonmlflow.IsAlreadyExistsError(createErr) {
		// Race-safe fallback: another request created it between get-by-name and create.
		existing, err := requestCtx.Client.GetExperimentByName(ctx, experimentName)
		if err != nil {
			return nil, err
		}
		return &Experiment{ID: existing.ID, Name: existing.Name}, nil
	}
	return nil, createErr
}

// BuildKFPRunURL constructs a relative KFP pipeline run URL
func BuildKFPRunURL(runID string) string {
	if runID == "" {
		return ""
	}
	return fmt.Sprintf("/#/runs/details/%s", runID)
}

// TagRunWithKFPMetadata sets MLflow tags with KFP metadata on the given
// MLflow run.
func TagRunWithKFPMetadata(ctx context.Context, requestCtx *RequestContext, mlflowRunID string, run *apiv2beta1.Run) error {
	if run == nil {
		return util.NewInvalidInputError("run cannot be nil")
	}
	if requestCtx == nil || requestCtx.Client == nil {
		return util.NewInvalidInputError("MLflow request context is required")
	}
	tags := []commonmlflow.Tag{
		{Key: TagKFPRunID, Value: run.GetRunId()},
		{Key: TagKFPRunURL, Value: BuildKFPRunURL(run.GetRunId())},
	}
	if ref := run.GetPipelineVersionReference(); ref != nil {
		if ref.PipelineId != "" {
			tags = append(tags, commonmlflow.Tag{Key: TagKFPPipelineID, Value: ref.PipelineId})
		}
		if ref.PipelineVersionId != "" {
			tags = append(tags, commonmlflow.Tag{Key: TagKFPPipelineVersionID, Value: ref.PipelineVersionId})
		}
	}
	for _, tag := range tags {
		if err := requestCtx.Client.SetTag(ctx, mlflowRunID, tag.Key, tag.Value); err != nil {
			return util.Wrapf(err, "failed to set MLflow tag %q", tag.Key)
		}
	}
	return nil
}

func BuildRunURL(requestCtx *RequestContext, experimentID, runID string) string {
	if requestCtx == nil || requestCtx.BaseURL == nil || experimentID == "" || runID == "" {
		return ""
	}
	u := *requestCtx.BaseURL
	basePath := stringsTrimRightSlash(u.Path)
	u.Path = fmt.Sprintf("%s/experiments/%s/runs/%s", basePath, url.PathEscape(experimentID), url.PathEscape(runID))
	if requestCtx.WorkspacesEnabled && requestCtx.Workspace != "" {
		q := u.Query()
		q.Set("workspace", requestCtx.Workspace)
		u.RawQuery = q.Encode()
	}
	return u.String()
}

func SuccessfulPluginOutput(experimentID, experimentName, runID, runURL string) *apiv2beta1.PluginOutput {
	return buildPluginOutput(experimentID, experimentName, runID, runURL, apiv2beta1.PluginState_PLUGIN_SUCCEEDED, "")
}

func FailedPluginOutput(experimentID, experimentName, runID, runURL, stateMessage string) *apiv2beta1.PluginOutput {
	return buildPluginOutput(experimentID, experimentName, runID, runURL, apiv2beta1.PluginState_PLUGIN_FAILED, stateMessage)
}

// SetMLflowPluginOutput serializes the given PluginOutput into the model.Run's
// PluginsOutputString.
func SetMLflowPluginOutput(run *model.Run, pluginName string, output *apiv2beta1.PluginOutput) error {
	if run == nil || output == nil || pluginName == "" {
		return nil
	}
	apiRun := &apiv2beta1.Run{
		PluginsOutput: map[string]*apiv2beta1.PluginOutput{pluginName: output},
	}
	return SyncPluginOutputToModel(apiRun, run)
}

// GetRunPluginOutput deserializes a plugin's output from model.Run.
func GetRunPluginOutput(run *model.Run, pluginName string) (*apiv2beta1.PluginOutput, error) {
	if run == nil || run.PluginsOutputString == nil || *run.PluginsOutputString == "" || pluginName == "" {
		return nil, nil
	}
	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal([]byte(*run.PluginsOutputString), &raw); err != nil {
		return nil, util.NewInvalidInputError("plugins_output must be valid JSON before read: %v", err)
	}
	payload, ok := raw[pluginName]
	if !ok {
		return nil, nil
	}
	output := &apiv2beta1.PluginOutput{}
	if err := protojson.Unmarshal(payload, output); err != nil {
		return nil, util.NewInvalidInputError("plugins_output.%s must be valid PluginOutput JSON: %v", pluginName, err)
	}
	return output, nil
}

// ModelToPluginRun converts a model.Run to a minimal *apiv2beta1.Run.
func ModelToPluginRun(m *model.Run) *apiv2beta1.Run {
	if m == nil {
		return nil
	}
	r := &apiv2beta1.Run{
		RunId:       m.UUID,
		DisplayName: m.DisplayName,
	}
	// State
	if m.State != "" {
		if val, ok := apiv2beta1.RuntimeState_value[string(m.State)]; ok {
			r.State = apiv2beta1.RuntimeState(val)
		}
	}
	// Pipeline version reference
	if m.PipelineSpec.PipelineId != "" || m.PipelineSpec.PipelineVersionId != "" {
		r.PipelineSource = &apiv2beta1.Run_PipelineVersionReference{
			PipelineVersionReference: &apiv2beta1.PipelineVersionReference{
				PipelineId:        m.PipelineSpec.PipelineId,
				PipelineVersionId: m.PipelineSpec.PipelineVersionId,
			},
		}
	}
	if m.FinishedAtInSec > 0 {
		r.FinishedAt = timestamppb.New(time.Unix(m.FinishedAtInSec, 0))
	}
	// Deserialize plugins_output from model JSON to proto map
	if m.PluginsOutputString != nil && *m.PluginsOutputString != "" {
		rawMap := map[string]json.RawMessage{}
		if err := json.Unmarshal([]byte(*m.PluginsOutputString), &rawMap); err == nil {
			r.PluginsOutput = make(map[string]*apiv2beta1.PluginOutput)
			for key, payload := range rawMap {
				output := &apiv2beta1.PluginOutput{}
				if err := protojson.Unmarshal(payload, output); err == nil {
					r.PluginsOutput[key] = output
				}
			}
		}
	}
	return r
}

// SyncPluginOutputToModel writes the proto PluginsOutput back to the model, merging with existing entries.
func SyncPluginOutputToModel(apiRun *apiv2beta1.Run, modelRun *model.Run) error {
	if apiRun == nil || modelRun == nil || len(apiRun.PluginsOutput) == 0 {
		return nil
	}
	// Preserve existing plugin entries from the model
	raw := map[string]json.RawMessage{}
	if modelRun.PluginsOutputString != nil && *modelRun.PluginsOutputString != "" {
		_ = json.Unmarshal([]byte(*modelRun.PluginsOutputString), &raw)
	}
	// Overwrite with proto entries
	for key, output := range apiRun.PluginsOutput {
		marshaledOutput, err := protojson.Marshal(output)
		if err != nil {
			return fmt.Errorf("failed to marshal plugin output for %q: %w", key, err)
		}
		raw[key] = marshaledOutput
	}
	marshaledMap, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("failed to marshal plugins_output map: %w", err)
	}
	lt := model.LargeText(string(marshaledMap))
	modelRun.PluginsOutputString = &lt
	return nil
}

func GetStringEntry(output *apiv2beta1.PluginOutput, key string) string {
	if output == nil || output.Entries == nil || key == "" {
		return ""
	}
	entry, ok := output.Entries[key]
	if !ok || entry == nil || entry.Value == nil {
		return ""
	}
	return entry.Value.GetStringValue()
}

func GetParentRunID(output *apiv2beta1.PluginOutput) string {
	return GetStringEntry(output, EntryRootRunID)
}

func SetPluginOutputState(output *apiv2beta1.PluginOutput, state apiv2beta1.PluginState, stateMessage string) {
	if output == nil {
		return
	}
	output.State = state
	output.StateMessage = stateMessage
}

func ResolveExperimentDescription(explicit *string) *string {
	if explicit == nil {
		d := DefaultExperimentDescription
		return &d
	}
	if *explicit == "" {
		return nil
	}
	return explicit
}

// maxSearchPages caps SearchRuns pagination to prevent infinite loops.
const maxSearchPages = 100

func SyncParentAndNestedRuns(ctx context.Context, requestCtx *RequestContext, parentRunID, experimentID string, mode RunSyncMode, terminalStatus string, endTimeMs *int64) []string {
	if requestCtx == nil || requestCtx.Client == nil {
		return []string{"MLflow request context is required"}
	}
	if parentRunID == "" {
		return []string{"MLflow parent run_id is required"}
	}
	targetStatus := terminalStatus
	parentAction := "update parent run status"
	nestedAction := "close nested run"
	switch mode {
	case RunSyncModeRetry:
		targetStatus = "RUNNING"
		parentAction = "reopen parent run"
		nestedAction = "reopen nested run"
	case RunSyncModeTerminal:
		// keep caller-provided terminal status
	default:
		return []string{fmt.Sprintf("unsupported MLflow run sync mode %q", mode)}
	}
	var syncErrors []string
	if err := requestCtx.Client.UpdateRun(ctx, parentRunID, targetStatus, endTimeMs); err != nil {
		syncErrors = append(syncErrors, fmt.Sprintf("failed to %s: %v", parentAction, err))
	}
	if experimentID == "" {
		return syncErrors
	}
	filter := fmt.Sprintf("tags.mlflow.parentRunId = '%s'", parentRunID)
	pageToken := ""
	for page := 0; page < maxSearchPages; page++ {
		searchResp, err := requestCtx.Client.SearchRuns(ctx, []string{experimentID}, filter, 1000, pageToken)
		if err != nil {
			syncErrors = append(syncErrors, fmt.Sprintf("failed to search nested runs: %v", err))
			break
		}
		for _, runPayload := range searchResp.Runs {
			mlflowRun := &searchRunPayload{}
			if err := json.Unmarshal(runPayload, mlflowRun); err != nil {
				syncErrors = append(syncErrors, fmt.Sprintf("failed to decode nested run payload: %v", err))
				continue
			}
			nestedRunID := mlflowRun.Info.RunID
			if nestedRunID == "" {
				nestedRunID = mlflowRun.Info.RunUUID
			}
			if nestedRunID == "" || nestedRunID == parentRunID || !shouldSyncNestedRun(mode, mlflowRun.Info.Status) {
				continue
			}
			if err := requestCtx.Client.UpdateRun(ctx, nestedRunID, targetStatus, endTimeMs); err != nil {
				syncErrors = append(syncErrors, fmt.Sprintf("failed to %s %s: %v", nestedAction, nestedRunID, err))
			}
		}
		if searchResp.NextPageToken == "" {
			break
		}
		pageToken = searchResp.NextPageToken
	}
	return syncErrors
}

func buildPluginOutput(experimentID, experimentName, runID, runURL string, state apiv2beta1.PluginState, stateMessage string) *apiv2beta1.PluginOutput {
	entries := map[string]*apiv2beta1.MetadataValue{}
	if experimentName != "" {
		entries[EntryExperimentName] = &apiv2beta1.MetadataValue{Value: structpb.NewStringValue(experimentName)}
	}
	if experimentID != "" {
		entries[EntryExperimentID] = &apiv2beta1.MetadataValue{Value: structpb.NewStringValue(experimentID)}
	}
	if runID != "" {
		entries[EntryRootRunID] = &apiv2beta1.MetadataValue{Value: structpb.NewStringValue(runID)}
	}
	if runURL != "" {
		entries[EntryRunURL] = &apiv2beta1.MetadataValue{
			Value:      structpb.NewStringValue(runURL),
			RenderType: apiv2beta1.MetadataValue_URL.Enum(),
		}
	}
	return &apiv2beta1.PluginOutput{
		Entries:      entries,
		State:        state,
		StateMessage: stateMessage,
	}
}

func stringsTrimRightSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

func shouldSyncNestedRun(mode RunSyncMode, status string) bool {
	upperStatus := strings.ToUpper(status)
	switch mode {
	case RunSyncModeTerminal:
		return upperStatus == "RUNNING" || upperStatus == "SCHEDULED"
	case RunSyncModeRetry:
		return upperStatus == "FAILED" || upperStatus == "KILLED"
	default:
		return false
	}
}

type searchRunPayload struct {
	Info struct {
		RunID   string `json:"run_id"`
		RunUUID string `json:"run_uuid"`
		Status  string `json:"status"`
	} `json:"info"`
}
