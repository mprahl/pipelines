package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/kubeflow/pipelines/backend/src/apiserver/template"
	k8sapi "github.com/kubeflow/pipelines/kubernetes_api/v2beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var scheme *runtime.Scheme

func init() {
	scheme = runtime.NewScheme()

	err := k8sapi.AddToScheme(scheme)
	if err != nil {
		// Panic is okay here because it means there's a code issue and so the package shouldn't initialize.
		panic(fmt.Sprintf("Failed to initialize the Kubernetes API scheme: %v", err))
	}
}

type PipelineVersionsWebhook struct {
	Client ctrlclient.Client
}

var _ ctrladmission.CustomDefaulter = &PipelineVersionsWebhook{}

func newBadRequestError(msg string) *apierrors.StatusError {
	return &apierrors.StatusError{
		ErrStatus: metav1.Status{
			Code:    http.StatusBadRequest,
			Reason:  metav1.StatusReasonBadRequest,
			Message: msg,
		},
	}
}

func (p *PipelineVersionsWebhook) ValidateCreate(
	ctx context.Context, obj runtime.Object,
) (warnings ctrladmission.Warnings, err error) {
	pipelineVersion, ok := obj.(*k8sapi.PipelineVersion)
	if !ok {
		return nil, newBadRequestError(fmt.Sprintf("Expected a PipelineVersion object but got %T", pipelineVersion))
	}

	pipeline := &k8sapi.Pipeline{}

	err = p.Client.Get(
		ctx, types.NamespacedName{Namespace: pipelineVersion.Namespace, Name: pipelineVersion.Spec.PipelineName}, pipeline,
	)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, newBadRequestError("The spec.pipelineName doesn't map to an existing Pipeline object")
		}

		return nil, err
	}

	pipelineSpec, err := json.Marshal(pipelineVersion.Spec.PipelineSpec)
	if err != nil {
		return nil, newBadRequestError(fmt.Sprintf("The pipeline spec is invalid JSON: %v", err))
	}

	tmpl, err := template.NewV2SpecTemplate(pipelineSpec)
	if err != nil {
		return nil, newBadRequestError(fmt.Sprintf("The pipeline spec is invalid: %v", err))
	}

	err = common.ValidatePipelineName(tmpl.V2PipelineName())
	if err != nil {
		return nil, newBadRequestError(err.Error())
	}

	if pipelineVersion.Name != tmpl.V2PipelineName() {
		return nil, newBadRequestError("The object name must match spec.pipelineSpec.pipelineInformation.name")
	}

	return nil, nil
}

func (p *PipelineVersionsWebhook) ValidateUpdate(
	ctx context.Context, oldObj, newObj runtime.Object,
) (warnings ctrladmission.Warnings, err error) {
	oldPipelineVersion, ok := oldObj.(*k8sapi.PipelineVersion)
	if !ok {
		return nil, newBadRequestError(fmt.Sprintf("Expected a PipelineVersion object but got %T", oldPipelineVersion))
	}

	newPipelineVersion, ok := newObj.(*k8sapi.PipelineVersion)
	if !ok {
		return nil, newBadRequestError(fmt.Sprintf("Expected a PipelineVersion object but got %T", newPipelineVersion))
	}

	if !reflect.DeepEqual(oldPipelineVersion.Spec, newPipelineVersion.Spec) {
		return nil, newBadRequestError("The pipeline spec field is immutable")
	}

	return nil, nil
}

func (p *PipelineVersionsWebhook) ValidateDelete(
	ctx context.Context, obj runtime.Object,
) (warnings ctrladmission.Warnings, err error) {
	return nil, nil
}

func (p *PipelineVersionsWebhook) Default(ctx context.Context, obj runtime.Object) error {
	pipelineVersion, ok := obj.(*k8sapi.PipelineVersion)
	if !ok {
		return &apierrors.StatusError{
			ErrStatus: metav1.Status{
				Code:    http.StatusBadRequest,
				Reason:  metav1.StatusReasonBadRequest,
				Message: fmt.Sprintf("expected a PipelineVersion object but got %T", pipelineVersion),
			},
		}
	}

	pipeline := &k8sapi.Pipeline{}

	err := p.Client.Get(ctx, types.NamespacedName{Namespace: pipelineVersion.Namespace, Name: pipelineVersion.Spec.PipelineName}, pipeline)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return newBadRequestError("The spec.pipelineName doesn't map to an existing Pipeline object")
		}

		return err
	}

	if pipelineVersion.Labels == nil {
		pipelineVersion.Labels = map[string]string{}
	}

	pipelineVersion.Labels["pipelines.kubeflow.org/pipeline-id"] = string(pipeline.UID)

	needsUpdate := false

	for i := range pipelineVersion.OwnerReferences {
		ownerRef := &pipelineVersion.OwnerReferences[i]
		if ownerRef.APIVersion != k8sapi.GroupVersion.String() || ownerRef.Kind != "Pipeline" {
			continue
		}

		if ownerRef.Name != pipeline.Name || ownerRef.UID != pipeline.UID {
			ownerRef.Name = pipeline.Name
			ownerRef.UID = pipeline.UID
			needsUpdate = true
		} else {
			return nil
		}
	}

	if !needsUpdate {
		pipelineVersion.OwnerReferences = append(pipelineVersion.OwnerReferences, metav1.OwnerReference{
			APIVersion: k8sapi.GroupVersion.String(),
			Kind:       "Pipeline",
			Name:       pipeline.Name,
			UID:        pipeline.UID,
		})
	}

	return nil
}

func NewPipelineVersionWebhook(client ctrlclient.Client) (http.Handler, http.Handler, error) {
	validating, err := ctrladmission.StandaloneWebhook(
		ctrladmission.WithCustomValidator(scheme, &k8sapi.PipelineVersion{}, &PipelineVersionsWebhook{Client: client}),
		ctrladmission.StandaloneOptions{},
	)
	if err != nil {
		return nil, nil, err
	}

	mutating, err := ctrladmission.StandaloneWebhook(
		ctrladmission.WithCustomDefaulter(scheme, &k8sapi.PipelineVersion{}, &PipelineVersionsWebhook{Client: client}),
		ctrladmission.StandaloneOptions{},
	)
	if err != nil {
		return nil, nil, err
	}

	return validating, mutating, nil
}
