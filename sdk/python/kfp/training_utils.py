from typing import Callable, List
from kfp import dsl


def submit_training_job(
    train_func: Callable,
    runtime_ref: str,
    num_nodes: int = 1,
    packages_to_install: List[str] = None,
    run_name: str = None,
    kubernetes_config: dsl.TaskKubernetesConfig = None,
    train_job_patch: dict = None,
    **kwargs,
):
    import inspect
    import textwrap
    import json
    import uuid
    import time
    import logging
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException
    import kubernetes.config as config

    if kubernetes_config is None:
        kubernetes_config = dsl.TaskKubernetesConfig()

    if run_name is None:        
        run_name = f"kfp-{uuid.uuid4()}"

    if train_job_patch is None:
        train_job_patch = {}

    with open(
        "/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r"
    ) as ns_file:
        namespace = ns_file.readline()

    print("Generating command...")

    func_code = inspect.getsource(train_func)
    func_code = textwrap.dedent(func_code)

    func_call_code = f"""
import os
import json

# Parse function arguments from environment variable
config_json = os.environ.get("TRAINING_CONFIG", "{{}}")
func_args = json.loads(config_json)

# Call the training function with parsed arguments
{train_func.__name__}(**func_args)
"""

    func_code = f"{func_code}\n{func_call_code}"

    packages_str = ""
    if packages_to_install:
        packages_str = f"""

if ! [ -x "$(command -v pip)" ]; then
echo "Installing pip..."
python -m ensurepip || python -m ensurepip --user
fi

echo "Installing Python packages..."
PIP_DISABLE_PIP_VERSION_CHECK=1 python -m pip install --user --quiet --no-warn-script-location {" ".join(packages_to_install)}
"""

    install_script = f"""set -e
set -o pipefail

echo "=== Starting container setup ==="
echo "Python version: $(python --version)"
{packages_str}

echo "Creating training script..."
cat > ephemeral_component.py << 'EOF'
{func_code}
EOF

echo "Starting distributed training..."
torchrun ephemeral_component.py"""

    command = ["bash", "-c", install_script]

    print(f"Generated command: {command}")
    print(f"Command length: {len(command)}")
    print(f"Command type: {type(command)}")

    print("Loading Kubernetes configuration...")
    try:
        config.load_incluster_config()
        print("Loaded in-cluster Kubernetes configuration")
    except config.ConfigException:
        config.load_kube_config()
        print("Loaded kubeconfig Kubernetes configuration")

    print("Creating Kubernetes API client...")
    api_client = k8s_client.ApiClient()
    custom_objects_api = k8s_client.CustomObjectsApi(api_client)
    print("Successfully created Kubernetes API client")

    print("Defining TrainJob resource...")

    train_job = {
        "apiVersion": "trainer.kubeflow.org/v1alpha1",
        "kind": "TrainJob",
        "metadata": {"name": run_name, "namespace": namespace},
        "spec": {
            "runtimeRef": {"name": runtime_ref},
            "trainer": {
                "numNodes": num_nodes,
                "resourcesPerNode": kubernetes_config.resources,
                "env": kubernetes_config.env,
                "command": command,
            },
            "podSpecOverrides": [
                {
                    "targetJobs": [{"name": "node"}],
                    "volumes": kubernetes_config.volumes,
                    "containers": [
                        {
                            "name": "node",
                            "volumeMounts": kubernetes_config.volume_mounts,
                        }
                    ],
                    "nodeSelector": kubernetes_config.node_selector,
                    "tolerations": kubernetes_config.tolerations,
                }
            ],
        },
    }

    if kubernetes_config.image_pull_secrets:
        logging.warning("Image pull secrets are not supported for training jobs")

    if kubernetes_config.affinity:
        logging.warning("Affinity is not supported for training jobs")

    print(f"TrainJob definition created:")
    print(f"  - Name: {run_name}")
    print(f"  - Namespace: {namespace}")

    print("Submitting TrainJob to Kubernetes...")
    try:
        response = custom_objects_api.create_namespaced_custom_object(
            group="trainer.kubeflow.org",
            version="v1alpha1",
            namespace=namespace,
            plural="trainjobs",
            body=train_job,
        )
        job_name = response["metadata"]["name"]
        print(f"TrainJob {job_name} created successfully")
        print(f"Response metadata: {response.get('metadata', {})}")
    except ApiException as e:
        print(f"Error creating TrainJob: {e}")
        print(f"Error details: {e.body}")
        print(f"Error status: {e.status}")
        raise

    print(f"Starting to monitor TrainJob {job_name} status...")
    check_count = 0
    while True:
        check_count += 1
        try:
            print(f"Checking job status (attempt {check_count})...")
            job_status = custom_objects_api.get_namespaced_custom_object(
                group="trainer.kubeflow.org",
                version="v1alpha1",
                namespace=namespace,
                plural="trainjobs",
                name=job_name,
            )

            status = job_status.get("status", {})
            conditions = status.get("conditions", [])
            print(f"Job status conditions: {conditions}")

            completed = False
            failed = False

            for condition in conditions:
                condition_type = condition.get("type", "")
                condition_status = condition.get("status", "")
                condition_reason = condition.get("reason", "")
                condition_message = condition.get("message", "")

                print(
                    f"Condition: type={condition_type}, status={condition_status}, reason={condition_reason}"
                )

                if condition_type == "Complete" and condition_status == "True":
                    print(
                        f"Training job {job_name} completed successfully: {condition_message}"
                    )
                    completed = True
                    break
                elif condition_type == "Failed" and condition_status == "True":
                    print(f"Training job {job_name} failed: {condition_message}")
                    failed = True
                    break
                elif condition_type == "Cancelled" and condition_status == "True":
                    print(f"Training job {job_name} was cancelled: {condition_message}")
                    failed = True
                    break

            if completed:
                break
            elif failed:
                raise RuntimeError(f"Training job {job_name} failed or was cancelled")
            else:
                print(f"Job is still running, continuing to wait...")

        except ApiException as e:
            print(f"Error checking job status: {e}")
            print(f"Error details: {e.body}")

        print(f"Waiting 10 seconds before next check...")
        time.sleep(10)
