from typing import Callable, List
from kfp import dsl


def _deep_merge_dicts(base: dict, patch: dict) -> dict:
    """Deep-merge patch into base, merging dicts and intelligently merging lists.

    - Dicts: merged recursively.
    - Lists of dicts with a "name" key: merged by name with deep-merge of items.
    - Other lists: append non-duplicates while preserving order.
    """

    def _is_named_dict(value):
        return isinstance(value, dict) and "name" in value

    def _merge_lists(base_list, patch_list):
        patch_has_named = any(_is_named_dict(item) for item in patch_list)
        if patch_has_named:
            name_to_index = {
                item["name"]: idx
                for idx, item in enumerate(base_list)
                if _is_named_dict(item)
            }
            for patch_item in patch_list:
                if _is_named_dict(patch_item):
                    item_name = patch_item["name"]
                    if item_name in name_to_index:
                        base_item = base_list[name_to_index[item_name]]
                        _deep_merge_dicts(base_item, patch_item)
                    else:
                        base_list.append(patch_item)
                else:
                    if patch_item not in base_list:
                        base_list.append(patch_item)
            return base_list
        for patch_item in patch_list:
            if patch_item not in base_list:
                base_list.append(patch_item)
        return base_list

    for key, patch_value in patch.items():
        base_value = base.get(key)
        if isinstance(base_value, dict) and isinstance(patch_value, dict):
            _deep_merge_dicts(base_value, patch_value)
        elif isinstance(base_value, list) and isinstance(patch_value, list):
            _merge_lists(base_value, patch_value)
        else:
            base[key] = patch_value
    return base


def stream_main_worker_logs(api_client, namespace: str, run_name: str, stop_event):
    """Stream logs from the main worker pod (completion index 0) in background.

    Retries pod discovery and reconnects to log stream on errors/closures until
    stop_event is set. No exceptions are raised; errors are printed and retried.
    """
    from kubernetes import client as k8s_client  # local import to avoid hard dep at module load
    import time

    core_v1 = k8s_client.CoreV1Api(api_client)
    label_selector = (
        f"jobset.sigs.k8s.io/jobset-name={run_name},"
        f"batch.kubernetes.io/job-completion-index=0"
    )
    last_pod_name = None
    print(
        "Background log streamer started. Waiting for the main worker pod to appear..."
    )
    while not stop_event.is_set():
        try:
            pods = core_v1.list_namespaced_pod(
                namespace=namespace,
                label_selector=label_selector,
            ).items
            if not pods:
                time.sleep(5)
                continue
            pod = pods[0]
            pod_name = pod.metadata.name
            if pod_name != last_pod_name:
                print(f"Connecting to logs of pod {pod_name}")
                last_pod_name = pod_name

            # Only check the pod phase. Avoid reading logs while phase is Pending/Unknown
            status = getattr(pod, "status", None)
            phase = getattr(status, "phase", None) if status is not None else None
            if phase not in ("Running", "Succeeded", "Failed"):
                print(f"Pod {pod_name} phase is {phase or 'unknown'}; waiting before streaming logs...")
                time.sleep(5)
                continue

            try:
                resp = core_v1.read_namespaced_pod_log(
                    name=pod_name,
                    namespace=namespace,
                    follow=True,
                    _preload_content=False,
                )
                for line in resp.stream(decode_content=True):
                    if stop_event.is_set():
                        break
                    try:
                        text = line.decode("utf-8", errors="replace").rstrip()
                    except AttributeError:
                        text = str(line).rstrip()
                    if text:
                        print(f"[{pod_name}] {text}")
            except Exception as e:
                print(f"Error streaming logs from pod {pod_name}: {e}")
            time.sleep(5)
        except Exception as e:
            print(f"Error finding main worker pod: {e}")
            time.sleep(5)
    print("Background log streamer stopped.")


def monitor_train_job(custom_objects_api, namespace: str, job_name: str) -> None:
    """Monitor a TrainJob until completion or failure.

    Polls status every 10 seconds, printing condition updates. Returns on
    completion, raises RuntimeError on failure or cancellation.
    """
    import time
    from kubernetes.client.rest import ApiException

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
                return
            elif failed:
                raise RuntimeError(f"Training job {job_name} failed or was cancelled")
            else:
                print(f"Job is still running, continuing to wait...")

        except ApiException as e:
            print(f"Error checking job status: {e}")
            print(f"Error details: {e.body}")

        print(f"Waiting 10 seconds before next check...")
        time.sleep(10)


def build_train_job(
    *,
    runtime_ref: str,
    run_name: str,
    namespace: str,
    num_nodes: int,
    kubernetes_config: dsl.TaskKubernetesConfig,
    train_func: Callable,
    packages_to_install: List[str] = None,
    training_kwargs: dict,
    train_job_patch: dict = None,
) -> dict:
    """Construct the TrainJob custom resource body.

    Returns a dict ready to be submitted to the Kubernetes API.
    """
    import json
    import inspect
    import textwrap

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
        safe_packages = [str(p) for p in packages_to_install]
        packages_str = (
            "\n\nif ! [ -x \"$(command -v pip)\" ]; then\n"
            "echo \"Installing pip...\"\n"
            "python -m ensurepip || python -m ensurepip --user\n"
            "fi\n\n"
            "echo \"Installing Python packages...\"\n"
            f"PIP_DISABLE_PIP_VERSION_CHECK=1 python -m pip install --user --quiet --no-warn-script-location {' '.join(safe_packages)}\n"
        )

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

    env = kubernetes_config.env
    if env is None:
        env = []
    else:
        env = list(env)
    env.append({
        "name": "TRAINING_CONFIG",
        "value": json.dumps(training_kwargs),
    })

    home_set = False

    for env_var in env:
        if env_var["name"] == "HOME":
            home_set = True
            break

    if not home_set:
        env.append({
            "name": "HOME",
            "value": "/tmp",
        })

    train_job = {
        "apiVersion": "trainer.kubeflow.org/v1alpha1",
        "kind": "TrainJob",
        "metadata": {"name": run_name, "namespace": namespace},
        "spec": {
            "runtimeRef": {"name": runtime_ref},
            "trainer": {
                "numNodes": num_nodes,
                "resourcesPerNode": kubernetes_config.resources,
                "env": env,
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

    if train_job_patch:
        print("Merging user-provided train_job_patch into TrainJob spec...")
        train_job = _deep_merge_dicts(train_job, train_job_patch)

    return train_job


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
    import uuid
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
        namespace = ns_file.readline().strip()

    # Command construction is handled by build_train_job

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

    train_job = build_train_job(
        runtime_ref=runtime_ref,
        run_name=run_name,
        namespace=namespace,
        num_nodes=num_nodes,
        kubernetes_config=kubernetes_config,
        train_func=train_func,
        packages_to_install=packages_to_install,
        training_kwargs=kwargs,
        train_job_patch=train_job_patch,
    )

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

    # Start streaming logs from the main worker pod in the background
    import threading

    stop_log_stream_event = threading.Event()
    log_thread = threading.Thread(
        target=stream_main_worker_logs,
        args=(api_client, namespace, run_name, stop_log_stream_event),
        daemon=True,
    )
    print("Starting log streaming from main worker pod...")
    log_thread.start()

    print(f"Starting to monitor TrainJob {job_name} status...")
    try:
        monitor_train_job(custom_objects_api, namespace, job_name)
    finally:
        stop_log_stream_event.set()
        try:
            log_thread.join(timeout=10)
        except Exception:
            pass
