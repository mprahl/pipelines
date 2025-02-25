# Copyright 2023 The Kubeflow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""AutoML Split Materialized Data component spec."""

from kfp import dsl
from kfp.dsl import Artifact
from kfp.dsl import Dataset
from kfp.dsl import Input
from kfp.dsl import Output


@dsl.container_component
def split_materialized_data(
    materialized_data: Input[Dataset],
    materialized_train_split: Output[Artifact],
    materialized_eval_split: Output[Artifact],
    materialized_test_split: Output[Artifact],
):
  # fmt: off
  """Splits materialized dataset into train, eval, and test data splits.

  The materialized dataset generated by the Feature Transform Engine consists of
  all the splits
  that were combined into the input transform dataset (i.e., train, eval, and
  test splits).
  This components splits the output materialized dataset into corresponding
  materialized data splits
  so that the splits can be used by down-stream training or evaluation
  components.

  Args:
      materialized_data: Materialized dataset output by the Feature
        Transform Engine.

  Returns:
      materialized_train_split: Path patern to materialized train split.
      materialized_eval_split: Path patern to materialized eval split.
      materialized_test_split: Path patern to materialized test split.
  """
  # fmt: on

  return dsl.ContainerSpec(
      image='us-docker.pkg.dev/vertex-ai/automl-tabular/dataflow-worker:20250129_0625',
      command=[
          'sh',
          '-ec',
          (
              'program_path=$(mktemp -d)\nprintf "%s" "$0" >'
              ' "$program_path/ephemeral_component.py"\npython3 -m'
              ' kfp.components.executor_main                        '
              ' --component_module_path                        '
              ' "$program_path/ephemeral_component.py"                        '
              ' "$@"\n'
          ),
          (
              '\nimport kfp\nfrom kfp import dsl\nfrom kfp.dsl import *\nfrom'
              ' typing import *\n\ndef _split_materialized_data(\n   '
              ' materialized_data: Input[Dataset],\n   '
              " materialized_train_split: OutputPath('MaterializedSplit'),\n   "
              " materialized_eval_split: OutputPath('MaterializedSplit'),\n   "
              " materialized_test_split: OutputPath('MaterializedSplit')):\n "
              ' """Splits materialized_data into materialized_data test,'
              ' train, and eval splits.\n\n  Necessary adapter between FTE'
              ' pipeline and trainer.\n\n  Args:\n    materialized_data:'
              ' materialized_data dataset output by FTE.\n   '
              ' materialized_train_split: Path patern to'
              ' materialized_train_split.\n    materialized_eval_split: Path'
              ' patern to materialized_eval_split.\n   '
              ' materialized_test_split: Path patern to'
              ' materialized_test_split.\n  """\n  # pylint:'
              ' disable=g-import-not-at-top,import-outside-toplevel,redefined-outer-name,reimported\n'
              '  import json\n  import tensorflow as tf\n  # pylint:'
              ' enable=g-import-not-at-top,import-outside-toplevel,redefined-outer-name,reimported\n\n'
              "  with tf.io.gfile.GFile(materialized_data.path, 'r') as f:\n   "
              ' artifact_path = f.read()\n\n  # needed to import tf because'
              ' this is a path in gs://\n  with'
              " tf.io.gfile.GFile(artifact_path, 'r') as f:\n   "
              ' materialized_data_json = json.load(f)\n\n  if'
              " 'tf_record_data_source' in materialized_data_json:\n   "
              ' file_patterns ='
              " materialized_data_json['tf_record_data_source'][\n       "
              " 'file_patterns']\n  elif 'avro_data_source' in"
              ' materialized_data_json:\n    file_patterns ='
              " materialized_data_json['avro_data_source'][\n       "
              " 'file_patterns']\n  elif 'parquet_data_source' in"
              ' materialized_data_json:\n    file_patterns ='
              " materialized_data_json['parquet_data_source'][\n       "
              " 'file_patterns']\n  else:\n    raise ValueError(f'Unsupported"
              " training data source: {materialized_data_json}')\n\n  # we map"
              ' indices to file patterns based on the ordering of insertion'
              ' order\n  # in our transform_data (see above in'
              ' _generate_analyze_and_transform_data)\n  with'
              " tf.io.gfile.GFile(materialized_train_split, 'w') as f:\n   "
              ' f.write(file_patterns[0])\n\n  with'
              " tf.io.gfile.GFile(materialized_eval_split, 'w') as f:\n   "
              ' f.write(file_patterns[1])\n\n  with'
              " tf.io.gfile.GFile(materialized_test_split, 'w') as f:\n   "
              ' f.write(file_patterns[2])\n\n'
          ),
      ],
      args=[
          '--executor_input',
          '{{$}}',
          '--function_to_execute',
          '_split_materialized_data',
      ],
  )
