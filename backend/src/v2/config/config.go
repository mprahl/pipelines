package config

import (
	"encoding/json"

	"github.com/golang/glog"
	commonmlflow "github.com/kubeflow/pipelines/backend/src/common/mlflow"
	"github.com/spf13/viper"
)

const (
	kfpMLflowConfig = "KFP_MLFLOW_CONFIG"
)

const (
	mlFlowTrackingToken    = "MLFLOW_TRACKING_TOKEN"
	mlFlowTrackingUsername = "MLFLOW_TRACKING_USERNAME"
	mlflowTrackingPassword = "MLFLOW_TRACKING_PASSWORD"
)

const (
	mlflowRunID = "MLFLOW_RUN_ID"
)

func GetStringConfig(configName string) string {
	if !viper.IsSet(configName) {
		glog.Errorf("config %s not set", configName)
	}
	return viper.GetString(configName)
}

func GetKfpMLflowRuntimeConfig() string {
	return GetStringConfig(kfpMLflowConfig)
}

func GetMLflowRunID() string {
	return GetStringConfig(mlflowRunID)
}

func GetMLflowTrackingToken() string {
	return GetStringConfig(mlFlowTrackingToken)
}

func GetMLflowTrackingUsername() string {
	return GetStringConfig(mlFlowTrackingUsername)
}

func GetMLflowTrackingPassword() string {
	return GetStringConfig(mlflowTrackingPassword)
}

func FormatKfpMLflowRuntimeConfig() (commonmlflow.MLflowRuntimeConfig, error) {
	var cfg commonmlflow.MLflowRuntimeConfig
	if err := json.Unmarshal([]byte(GetKfpMLflowRuntimeConfig()), &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}
