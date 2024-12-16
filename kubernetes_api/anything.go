package kubernetesapi

import (
	"bytes"
	"encoding/json"

	"k8s.io/apimachinery/pkg/runtime"
)

// Taken from Gatekeeper:
// https://github.com/open-policy-agent/frameworks/blob/6b55861b3fad83f4638ff259ccbd07bff931fd4b/constraint/pkg/core/templates/constrainttemplate_types.go#L130

// Anything is a struct wrapper around a field of type `interface{}`
// that plays nicely with controller-gen
// +kubebuilder:object:generate=false
// +kubebuilder:validation:Type=""
type Anything struct {
	Value interface{} `json:"-"`
}

func (in *Anything) GetValue() interface{} {
	return runtime.DeepCopyJSONValue(in.Value)
}

func (in *Anything) UnmarshalJSON(val []byte) error {
	if bytes.Equal(val, []byte("null")) {
		return nil
	}
	return json.Unmarshal(val, &in.Value)
}

// MarshalJSON should be implemented against a value
// per http://stackoverflow.com/questions/21390979/custom-marshaljson-never-gets-called-in-go
// credit to K8s api machinery's RawExtension for finding this.
func (in Anything) MarshalJSON() ([]byte, error) {
	if in.Value == nil {
		return []byte("null"), nil
	}
	return json.Marshal(in.Value)
}

func (in *Anything) DeepCopy() *Anything {
	if in == nil {
		return nil
	}

	return &Anything{Value: runtime.DeepCopyJSONValue(in.Value)}
}

func (in *Anything) DeepCopyInto(out *Anything) {
	*out = *in

	if in.Value != nil {
		out.Value = runtime.DeepCopyJSONValue(in.Value)
	}
}
