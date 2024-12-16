package v2beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
)

type PipelineSpec struct {
	Description string `json:"description,omitempty"`
}

type PipelineStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

type Pipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PipelineSpec   `json:"spec,omitempty"`
	Status PipelineStatus `json:"status,omitempty"`
}

func FromPipelineModel(pipeline model.Pipeline) Pipeline {
	return Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pipeline.Name,
			Namespace: pipeline.Namespace,
			UID:       types.UID(pipeline.UUID),
		},
		Spec: PipelineSpec{
			Description: pipeline.Description,
		},
		Status: PipelineStatus{
			Conditions: []metav1.Condition{
				{
					Type:               "PipelineStatus",
					Reason:             string(pipeline.Status),
					Message:            string(pipeline.Status),
					LastTransitionTime: metav1.Now(),
					Status:             metav1.ConditionTrue,
				},
			},
		},
	}
}

func (p *Pipeline) ToModel() *model.Pipeline {
	pipelineStatus := model.PipelineCreating

	for _, condition := range p.Status.Conditions {
		if condition.Type == "PipelineStatus" {
			pipelineStatus = model.PipelineStatus(condition.Reason)

			break
		}
	}

	return &model.Pipeline{
		Name:        p.Name,
		Description: p.Spec.Description,
		Namespace:   p.Namespace,
		UUID:        string(p.UID),
		// TODO: Is this the expected format?
		CreatedAtInSec: p.CreationTimestamp.Unix(),
		Status:         pipelineStatus,
	}
}

// +kubebuilder:object:root=true

type PipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Pipeline `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Pipeline{}, &PipelineList{})
}
