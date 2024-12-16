package v2beta1

import (
	"encoding/json"
	"fmt"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kubeflow/pipelines/api/v2alpha1/go/pipelinespec"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	kubernetesapi "github.com/kubeflow/pipelines/kubernetes_api"
)

type PipelineVersionSpec struct {
	Description   string `json:"description,omitempty"`
	CodeSourceURL string `json:"codeSourceURL,omitempty"`
	PipelineName  string `json:"pipelineName,omitempty"`
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// Todo
	PipelineSpec kubernetesapi.Anything `json:"pipelineSpec"`
}

func ToPipelineSpec(p *kubernetesapi.Anything) (*pipelinespec.PipelineSpec, error) {
	pDefBytes, err := json.Marshal(p.Value)
	if err != nil {
		return nil, err
	}

	rv := &pipelinespec.PipelineSpec{}
	err = json.Unmarshal(pDefBytes, rv)
	if err != nil {
		return nil, err
	}

	return rv, nil
}

// SimplifiedCondition is a metav1.Condition without lastTransitionTime since the database model doesn't have such
// a concept and it allows a default status in the CRD without a controller setting it.
type SimplifiedCondition struct {
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$`
	// +kubebuilder:validation:MaxLength=316
	Type string `json:"type" protobuf:"bytes,1,opt,name=type"`
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=True;False;Unknown
	Status metav1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status"`
	Reason string                 `json:"reason" protobuf:"bytes,5,opt,name=reason"`
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=32768
	Message string `json:"message" protobuf:"bytes,6,opt,name=message"`
}

type PipelineVersionStatus struct {
	Conditions []SimplifiedCondition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

type PipelineVersion struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PipelineVersionSpec   `json:"spec,omitempty"`
	Status PipelineVersionStatus `json:"status,omitempty"`
}

func FromPipelineVersionModel(pipeline model.Pipeline, pipelineVersion model.PipelineVersion) (*PipelineVersion, error) {
	pipelineSpec := kubernetesapi.Anything{}

	err := yaml.Unmarshal([]byte(pipelineVersion.PipelineSpec), &pipelineSpec.Value)
	if err != nil {
		return nil, fmt.Errorf("the pipeline spec is invalid YAML: %w", err)
	}

	return &PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pipelineVersion.Name,
			Namespace: pipeline.Namespace,
			UID:       types.UID(pipelineVersion.UUID),
			Labels: map[string]string{
				"pipelines.kubeflow.org/pipeline-id": pipeline.UUID,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: GroupVersion.String(),
					Kind:       "Pipeline",
					UID:        types.UID(pipeline.UUID),
					Name:       pipeline.Name,
				},
			},
		},
		Spec: PipelineVersionSpec{
			Description:   pipelineVersion.Description,
			PipelineSpec:  pipelineSpec,
			PipelineName:  pipeline.Name,
			CodeSourceURL: pipelineVersion.CodeSourceUrl,
		},
	}, nil
}

func (p *PipelineVersion) ToModel() (*model.PipelineVersion, error) {
	pipelineSpec, err := json.Marshal(p.Spec.PipelineSpec)
	if err != nil {
		return nil, fmt.Errorf("the pipeline spec is invalid JSON: %w", err)
	}

	pipelineVersionStatus := model.PipelineVersionCreating

	for _, condition := range p.Status.Conditions {
		if condition.Type == "PipelineVersionStatus" {
			pipelineVersionStatus = model.PipelineVersionStatus(condition.Reason)

			break
		}
	}

	var pipelineID types.UID

	for _, ref := range p.OwnerReferences {
		if ref.Kind == "Pipeline" && ref.APIVersion == GroupVersion.String() {
			pipelineID = ref.UID

			break
		}
	}

	return &model.PipelineVersion{
		UUID:           string(p.UID),
		CreatedAtInSec: p.CreationTimestamp.Unix(),
		Name:           p.Name,
		PipelineId:     string(pipelineID),
		Description:    p.Spec.Description,
		PipelineSpec:   string(pipelineSpec),
		Status:         pipelineVersionStatus,
		CodeSourceUrl:  p.Spec.CodeSourceURL,
	}, nil
}

func (p *PipelineVersion) IsOwnedByPipeline(pipelineId string) bool {
	for _, ownerRef := range p.OwnerReferences {
		if string(ownerRef.UID) == pipelineId {
			return true
		}
	}

	return false
}

// +kubebuilder:object:root=true

type PipelineVersionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PipelineVersion `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PipelineVersion{}, &PipelineVersionList{})
}
