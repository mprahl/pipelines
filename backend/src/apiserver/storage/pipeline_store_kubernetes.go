package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/golang/glog"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/kubeflow/pipelines/backend/src/apiserver/list"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	k8sapi "github.com/kubeflow/pipelines/kubernetes_api/v2beta1"
	"github.com/pkg/errors"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	ErrNoV1             = errors.New("the v1 API is not available through Kubernetes")
	ErrUnsupportedField = errors.New("the field is unsupported")
)

type KubernetesPipelineStore struct {
	client ctrlclient.Client
}

// Factory function for pipeline store when stored as Kubernetes CRDs.
func NewKubernetesPipelineStore(kubernetesClient ctrlclient.Client) *KubernetesPipelineStore {
	return &KubernetesPipelineStore{client: kubernetesClient}
}

func (k *KubernetesPipelineStore) GetPipelineByNameAndNamespaceV1(name string, namespace string) (*model.Pipeline, *model.PipelineVersion, error) {
	return nil, nil, ErrNoV1
}

func (k *KubernetesPipelineStore) ListPipelinesV1(filterContext *model.FilterContext, opts *list.Options) ([]*model.Pipeline, []*model.PipelineVersion, int, string, error) {
	return nil, nil, 0, "", ErrNoV1
}

func (k *KubernetesPipelineStore) CreatePipelineAndPipelineVersion(pipeline *model.Pipeline, pipelineVersion *model.PipelineVersion) (*model.Pipeline, *model.PipelineVersion, error) {
	pipeline.UUID = ""
	pipelineVersion.UUID = ""

	var err error

	pipeline, err = k.CreatePipeline(pipeline)
	if err != nil {
		return nil, nil, err
	}

	pipelineVersion, err = k.createPipelineVersionWithPipeline(pipeline, pipelineVersion)
	if err != nil {
		return nil, nil, err
	}

	return pipeline, pipelineVersion, nil
}

func (k *KubernetesPipelineStore) CreatePipeline(pipeline *model.Pipeline) (*model.Pipeline, error) {
	pipeline.UUID = ""

	if pipeline.Parameters != "" {
		return nil, util.NewBadRequestError(ErrUnsupportedField, "The parameters field is not supported")
	}

	if pipeline.Namespace == "" {
		if common.IsMultiUserMode() || common.GetPodNamespace() == "" {
			return nil, util.NewBadRequestError(errors.New("A namespace is required"), "")
		}

		pipeline.Namespace = common.GetPodNamespace()
	}

	pipeline.Status = model.PipelineCreating

	k8sPipeline := k8sapi.FromPipelineModel(*pipeline)

	glog.Infof("Creating the pipeline %s/%s in Kubernetes", k8sPipeline.Namespace, k8sPipeline.Name)

	// TODO: Verify that a copy is required to see if Create wipes this field
	pipelineStatus := k8sPipeline.Status.DeepCopy()

	err := k.client.Create(context.TODO(), &k8sPipeline)
	if k8serrors.IsAlreadyExists(err) {
		return nil, util.NewAlreadyExistError(
			"Failed to create a new pipeline. The name %v already exists. Please specify a new name", pipeline.Name,
		)
	} else if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create the pipeline")
	}

	k8sPipeline.Status = *pipelineStatus

	err = k.client.Status().Update(context.TODO(), &k8sPipeline)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to set the pipeline status")
	}

	return k8sPipeline.ToModel(), nil
}

func (k *KubernetesPipelineStore) createPipelineVersionWithPipeline(pipeline *model.Pipeline, pipelineVersion *model.PipelineVersion) (*model.PipelineVersion, error) {
	if pipelineVersion.PipelineSpecURI != "" {
		return nil, util.NewBadRequestError(ErrUnsupportedField, "The pipeline spec URI field is not supported")
	}

	k8sPipelineVersion, err := k8sapi.FromPipelineVersionModel(*pipeline, *pipelineVersion)
	if err != nil {
		return nil, util.NewBadRequestError(err, "Invalid pipeline spec")
	}

	pipelineVersionStatus := k8sPipelineVersion.Status.DeepCopy()

	glog.Infof(
		"Creating the pipeline version %s/%s in Kubernetes", k8sPipelineVersion.Namespace, k8sPipelineVersion.Name,
	)
	err = k.client.Create(context.TODO(), k8sPipelineVersion)
	if k8serrors.IsAlreadyExists(err) {
		return nil, util.NewAlreadyExistError(
			"Failed to create a new pipeline version. The name %v already exists. Please specify a new name",
			pipelineVersion.Name,
		)
	} else if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create the pipeline version")
	}

	k8sPipelineVersion.Status = *pipelineVersionStatus

	err = k.client.Status().Update(context.TODO(), k8sPipelineVersion)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to set the pipeline version status")
	}

	return k8sPipelineVersion.ToModel()
}

func (k *KubernetesPipelineStore) getK8sPipeline(pipelineId string) (*k8sapi.Pipeline, error) {
	// TODO: Consider making the ID namespace/name format to make querying efficient
	pipelines := k8sapi.PipelineList{}

	listOptions := []ctrlclient.ListOption{ctrlclient.UnsafeDisableDeepCopy}

	if !common.IsMultiUserMode() && common.GetPodNamespace() != "" {
		listOptions = append(listOptions, ctrlclient.InNamespace(common.GetPodNamespace()))
	}

	// Be careful, the deep copy is disabled here to reduce memory allocations
	err := k.client.List(context.TODO(), &pipelines, listOptions...)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to find the pipeline")
	}

	for _, k8sPipeline := range pipelines.Items {
		if string(k8sPipeline.UID) == pipelineId {
			return &k8sPipeline, nil
		}
	}

	return nil, util.NewResourceNotFoundError("Pipeline", pipelineId)
}

func (k *KubernetesPipelineStore) GetPipeline(pipelineId string) (*model.Pipeline, error) {
	k8sPipeline, err := k.getK8sPipeline(pipelineId)
	if err != nil {
		return nil, err
	}

	return k8sPipeline.ToModel(), nil
}

func (k *KubernetesPipelineStore) CreatePipelineVersion(pipelineVersion *model.PipelineVersion) (*model.PipelineVersion, error) {
	pipeline, err := k.GetPipeline(pipelineVersion.PipelineId)
	if err != nil {
		return nil, err
	}

	return k.createPipelineVersionWithPipeline(pipeline, pipelineVersion)
}

func (k *KubernetesPipelineStore) GetPipelineWithStatus(pipelineId string, status model.PipelineStatus) (*model.Pipeline, error) {
	pipeline, err := k.GetPipeline(pipelineId)
	if err != nil {
		return nil, err
	}

	if pipeline.Status != status {
		return nil, util.NewResourceNotFoundError("Pipeline", pipelineId)
	}

	return pipeline, nil
}

func (k *KubernetesPipelineStore) GetPipelineByNameAndNamespace(name string, namespace string) (*model.Pipeline, error) {
	k8sPipeline := k8sapi.Pipeline{}

	err := k.client.Get(context.TODO(), types.NamespacedName{Namespace: namespace, Name: name}, &k8sPipeline)
	if k8serrors.IsNotFound(err) {
		return nil, util.NewResourceNotFoundError("Namespace/Pipeline", fmt.Sprintf("%v/%v", namespace, name))
	} else if err != nil {
		return nil, util.NewInternalServerError(
			err, "Failed to get a pipeline with name %v and namespace %v", name, namespace,
		)
	}

	return nil, util.NewResourceNotFoundError("Namespace/Pipeline", fmt.Sprintf("%v/%v", namespace, name))
}

func (k *KubernetesPipelineStore) ListPipelines(filterContext *model.FilterContext, opts *list.Options) ([]*model.Pipeline, int, string, error) {
	// TODO: Handle opts (e.g. pagination)
	k8sPipelines := k8sapi.PipelineList{}

	listOptions := []ctrlclient.ListOption{ctrlclient.UnsafeDisableDeepCopy}

	if filterContext.ReferenceKey != nil && filterContext.ReferenceKey.Type == model.NamespaceResourceType {
		listOptions = append(listOptions, ctrlclient.InNamespace(filterContext.ReferenceKey.ID))
	}

	// Be careful, the deep copy is disabled here to reduce memory allocations
	err := k.client.List(context.TODO(), &k8sPipelines, listOptions...)
	if err != nil {
		return nil, 0, "", util.NewInternalServerError(
			err, "Failed to find the pipeline associated with this pipeline version",
		)
	}

	pipelines := make([]*model.Pipeline, 0, len(k8sPipelines.Items))

	for _, k8sPipeline := range k8sPipelines.Items {
		pipelines = append(pipelines, k8sPipeline.ToModel())
	}

	return pipelines, len(pipelines), "", nil
}

func (k *KubernetesPipelineStore) UpdatePipelineStatus(pipelineId string, status model.PipelineStatus) error {
	k8sPipeline, err := k.getK8sPipeline(pipelineId)
	if err != nil {
		return err
	}

	conditionSet := false

	for i := range k8sPipeline.Status.Conditions {
		condition := &k8sPipeline.Status.Conditions[i]
		if condition.Type == "PipelineStatus" {
			if condition.Reason == string(status) && condition.Message == condition.Reason {
				return nil
			}

			condition.Reason = string(status)
			condition.Message = string(status)
			condition.LastTransitionTime = metav1.Now()
			condition.Status = metav1.ConditionTrue

			conditionSet = true

			break
		}
	}

	if !conditionSet {
		k8sPipeline.Status.Conditions = append(k8sPipeline.Status.Conditions, metav1.Condition{
			Type:               "PipelineStatus",
			Reason:             string(status),
			Message:            string(status),
			LastTransitionTime: metav1.Now(),
			Status:             metav1.ConditionTrue,
		})
	}

	// TODO: Add resilience to resource version changing
	err = k.client.Status().Update(context.TODO(), k8sPipeline)
	if err != nil {
		return util.NewInternalServerError(err, "Failed to update the pipeline status")
	}

	return nil
}

func (k *KubernetesPipelineStore) DeletePipeline(pipelineId string) error {
	k8sPipeline, err := k.getK8sPipeline(pipelineId)
	if err != nil {
		if strings.Contains(err.Error(), "ResourceNotFoundError:") {
			return nil
		}
	}

	err = k.client.Delete(context.TODO(), k8sPipeline)
	if err != nil && !k8serrors.IsNotFound(err) {
		return util.NewInternalServerError(err, "Failed to delete the pipeline")
	}

	return nil
}

func (k *KubernetesPipelineStore) UpdatePipelineDefaultVersion(pipelineId string, versionId string) error {
	return util.NewBadRequestError(errors.New("pipeline default version is unsupported"),
		"pipeline default version is unsupported when storing in Kubernetes")
}

func (k *KubernetesPipelineStore) getK8sPipelineVersions() (*k8sapi.PipelineVersionList, error) {
	// TODO: Consider making the ID namespace/name format to make querying efficient
	pipelineVersions := k8sapi.PipelineVersionList{}

	listOptions := []ctrlclient.ListOption{ctrlclient.UnsafeDisableDeepCopy}

	if !common.IsMultiUserMode() && common.GetPodNamespace() != "" {
		listOptions = append(listOptions, ctrlclient.InNamespace(common.GetPodNamespace()))
	}

	// Be careful, the deep copy is disabled here to reduce memory allocations
	err := k.client.List(context.TODO(), &pipelineVersions, listOptions...)
	if err != nil {
		return nil, util.NewInternalServerError(
			err, "Failed to find the pipeline version associated with this pipeline version",
		)
	}

	return &pipelineVersions, nil
}

func (k *KubernetesPipelineStore) getK8sPipelineVersion(pipelineVersionId string) (*k8sapi.PipelineVersion, error) {
	pipelineVersions, err := k.getK8sPipelineVersions()
	if err != nil {
		return nil, err
	}

	for _, k8sPipelineVersion := range pipelineVersions.Items {
		if string(k8sPipelineVersion.UID) == pipelineVersionId {
			return &k8sPipelineVersion, nil
		}
	}

	return nil, util.NewResourceNotFoundError("PipelineVersion", pipelineVersionId)
}

func (k *KubernetesPipelineStore) GetPipelineVersion(pipelineVersionId string) (*model.PipelineVersion, error) {
	pipelineVersion, err := k.getK8sPipelineVersion(pipelineVersionId)
	if err != nil {
		return nil, err
	}

	return pipelineVersion.ToModel()
}

func (k *KubernetesPipelineStore) GetPipelineVersionWithStatus(pipelineVersionId string, status model.PipelineVersionStatus) (*model.PipelineVersion, error) {
	pipelineVersion, err := k.GetPipelineVersion(pipelineVersionId)
	if err != nil {
		return nil, err
	}

	if pipelineVersion.Status != status {
		return nil, util.NewResourceNotFoundError("PipelineVersion", pipelineVersionId)
	}

	return pipelineVersion, nil
}

func (k *KubernetesPipelineStore) GetLatestPipelineVersion(pipelineId string) (*model.PipelineVersion, error) {
	k8sPipelineVersions, err := k.getK8sPipelineVersions()
	if err != nil {
		return nil, err
	}

	var latestK8sPipelineVersion *k8sapi.PipelineVersion

	for _, k8sPipelineVersion := range k8sPipelineVersions.Items {
		if !k8sPipelineVersion.IsOwnedByPipeline(pipelineId) {
			continue
		}

		if latestK8sPipelineVersion == nil || k8sPipelineVersion.CreationTimestamp.Time.After(latestK8sPipelineVersion.CreationTimestamp.Time) {
			latestK8sPipelineVersion = &k8sPipelineVersion

			continue
		}
	}

	if latestK8sPipelineVersion == nil {
		return nil, util.NewResourceNotFoundError("PipelineVersion", "Latest")
	}

	return latestK8sPipelineVersion.ToModel()
}

func (k *KubernetesPipelineStore) ListPipelineVersions(pipelineId string, opts *list.Options) ([]*model.PipelineVersion, int, string, error) {
	// TODO: Handle opts (e.g. pagination)

	k8sPipelineVersions, err := k.getK8sPipelineVersions()
	if err != nil {
		return nil, 0, "", err
	}

	pipelineVersions := make([]*model.PipelineVersion, 0, len(k8sPipelineVersions.Items))

	for _, k8sPipelineVersion := range k8sPipelineVersions.Items {
		if k8sPipelineVersion.IsOwnedByPipeline(pipelineId) {
			pipelineVersion, err := k8sPipelineVersion.ToModel()
			if err != nil {
				return nil, 0, "", err
			}

			pipelineVersions = append(pipelineVersions, pipelineVersion)
		}
	}

	return pipelineVersions, len(pipelineVersions), "", nil
}

func (k *KubernetesPipelineStore) UpdatePipelineVersionStatus(pipelineVersionId string, status model.PipelineVersionStatus) error {
	k8sPipelineVersion, err := k.getK8sPipelineVersion(pipelineVersionId)
	if err != nil {
		return err
	}

	conditionSet := false

	for i := range k8sPipelineVersion.Status.Conditions {
		condition := &k8sPipelineVersion.Status.Conditions[i]

		if condition.Type == "PipelineVersionStatus" {
			if condition.Reason == string(status) && condition.Message == condition.Reason {
				return nil
			}

			condition.Reason = string(status)
			condition.Message = string(status)
			condition.Status = metav1.ConditionTrue

			conditionSet = true

			break
		}
	}

	if !conditionSet {
		k8sPipelineVersion.Status.Conditions = append(k8sPipelineVersion.Status.Conditions, k8sapi.SimplifiedCondition{
			Type:    "PipelineVersionStatus",
			Reason:  string(status),
			Message: string(status),
			Status:  metav1.ConditionTrue,
		})
	}

	// TODO: Add resilience to resource version changing
	err = k.client.Status().Update(context.TODO(), k8sPipelineVersion)
	if err != nil {
		return util.NewInternalServerError(err, "Failed to update the pipeline version status")
	}

	return nil
}

func (k *KubernetesPipelineStore) DeletePipelineVersion(pipelineVersionId string) error {
	k8sPipelineVersion, err := k.getK8sPipelineVersion(pipelineVersionId)
	if err != nil {
		if strings.Contains(err.Error(), "ResourceNotFoundError:") {
			return nil
		}
	}

	err = k.client.Delete(context.TODO(), k8sPipelineVersion)
	if err != nil && !k8serrors.IsNotFound(err) {
		return util.NewInternalServerError(err, "Failed to delete the pipeline version")
	}

	return nil
}
