# Copyright 2021-2022 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import dataclasses
import json
import inspect
import itertools
import pathlib
import re
import textwrap
from typing import (Any, Callable, Dict, List, Mapping, Optional, Tuple, Type,
                    Union)
import warnings

import docstring_parser
import kfp
from kfp import dsl
from kfp.dsl import container_component_artifact_channel
from kfp.dsl import container_component_class
from kfp.dsl import graph_component
from kfp.dsl import pipeline_config
from kfp.dsl import placeholders
from kfp.dsl import python_component
from kfp.dsl import structures
from kfp.dsl import task_final_status
from kfp.dsl.component_task_config import TaskConfigPassthrough
from kfp.dsl.task_config import TaskConfig
from kfp.dsl.types import artifact_types
from kfp.dsl.types import custom_artifact_types
from kfp.dsl.types import type_annotations
from kfp.dsl.types import type_utils

_DEFAULT_BASE_IMAGE = 'python:3.9'
SINGLE_OUTPUT_NAME = 'Output'


@dataclasses.dataclass
class ComponentInfo():
    """A dataclass capturing registered components.

    This will likely be subsumed/augmented with BaseComponent.
    """
    name: str
    function_name: str
    func: Callable
    target_image: str
    module_path: pathlib.Path
    component_spec: structures.ComponentSpec
    output_component_file: Optional[str] = None
    base_image: str = _DEFAULT_BASE_IMAGE
    packages_to_install: Optional[List[str]] = None
    pip_index_urls: Optional[List[str]] = None
    pip_trusted_hosts: Optional[List[str]] = None
    use_venv: bool = False
    task_config_passthroughs: Optional[List[TaskConfigPassthrough]] = None


# A map from function_name to components.  This is always populated when a
# module containing KFP components is loaded. Primarily used by KFP CLI
# component builder to package components in a file into containers.
REGISTERED_MODULES = None


def _python_function_name_to_component_name(name):
    name_with_spaces = re.sub(' +', ' ', name.replace('_', ' ')).strip(' ')
    return name_with_spaces[0].upper() + name_with_spaces[1:]


def make_index_url_options(pip_index_urls: Optional[List[str]],
                           pip_trusted_hosts: Optional[List[str]]) -> str:
    """Generates index URL options for the pip install command based on the
    provided pip_index_urls and pip_trusted_hosts.

    Args:
        pip_index_urls (Optional[List[str]]): Optional list of pip index URLs.
        pip_trusted_hosts (Optional[List[str]]): Optional list of pip trusted hosts.

    Returns:
        str:
            - An empty string if pip_index_urls is empty or None.
            - '--index-url url ' if pip_index_urls contains 1 URL.
            - The above followed by '--extra-index-url url ' for each additional URL in pip_index_urls
            if pip_index_urls contains more than 1 URL.
            - If pip_trusted_hosts is None:
                - The above followed by '--trusted-host url ' for each URL in pip_index_urls.
            - If pip_trusted_hosts is an empty List.
                - No --trusted-host information will be added
            - If pip_trusted_hosts contains any URLs:
                - The above followed by '--trusted-host url ' for each URL in pip_trusted_hosts.
    Note:
        In case pip_index_urls is not empty, the returned string will contain a space at the end.
    """
    if not pip_index_urls:
        return ''

    index_url = pip_index_urls[0]
    extra_index_urls = pip_index_urls[1:]

    options = [f'--index-url {index_url}']
    options.extend(f'--extra-index-url {extra_index_url}'
                   for extra_index_url in extra_index_urls)

    if pip_trusted_hosts is None:
        options.extend([f'--trusted-host {index_url}'])
        options.extend(f'--trusted-host {extra_index_url}'
                       for extra_index_url in extra_index_urls)
    elif len(pip_trusted_hosts) > 0:
        options.extend(f'--trusted-host {trusted_host}'
                       for trusted_host in pip_trusted_hosts)

    return ' '.join(options) + ' '


def make_pip_install_command(
    install_parts: List[str],
    index_url_options: str,
) -> str:
    concat_package_list = ' '.join(
        [repr(str(package)) for package in install_parts])
    return f'python3 -m pip install --quiet --no-warn-script-location {index_url_options}{concat_package_list}'


_install_python_packages_script_template = '''
if ! [ -x "$(command -v pip)" ]; then
    python3 -m ensurepip || python3 -m ensurepip --user || apt-get install python3-pip
fi

PIP_DISABLE_PIP_VERSION_CHECK=1 {pip_install_commands} && "$0" "$@"
'''

# Creates and activates a virtual environment in a temporary directory.
# The environment inherits the system site packages.
_use_venv_script_template = '''
export PIP_DISABLE_PIP_VERSION_CHECK=1
tmp=$(mktemp -d)
python3 -m venv "$tmp/venv" --system-site-packages
. "$tmp/venv/bin/activate"
'''


def _get_packages_to_install_command(
    kfp_package_path: Optional[str] = None,
    pip_index_urls: Optional[List[str]] = None,
    packages_to_install: Optional[List[str]] = None,
    install_kfp_package: bool = True,
    target_image: Optional[str] = None,
    pip_trusted_hosts: Optional[List[str]] = None,
    use_venv: bool = False,
) -> List[str]:
    packages_to_install = packages_to_install or []
    kfp_in_user_pkgs = any(pkg.startswith('kfp') for pkg in packages_to_install)
    # if the user doesn't say "don't install", they aren't building a
    # container component, and they haven't already specified a KFP dep
    # themselves, we install KFP for them
    inject_kfp_install = install_kfp_package and target_image is None and not kfp_in_user_pkgs
    if not inject_kfp_install and not packages_to_install:
        return []
    pip_install_strings = []
    index_url_options = make_index_url_options(pip_index_urls,
                                               pip_trusted_hosts)

    # Install packages before KFP. This allows us to
    # control where we source kfp-pipeline-spec.
    # This is particularly useful for development and
    # CI use-case when you want to install the spec
    # from source.
    if packages_to_install:
        user_packages_pip_install_command = make_pip_install_command(
            install_parts=packages_to_install,
            index_url_options=index_url_options,
        )
        pip_install_strings.append(user_packages_pip_install_command)
        if inject_kfp_install:
            pip_install_strings.append(' && ')

    if inject_kfp_install:
        if use_venv:
            pip_install_strings.append(_use_venv_script_template)
        if kfp_package_path:
            kfp_pip_install_command = make_pip_install_command(
                install_parts=[kfp_package_path],
                index_url_options=index_url_options,
            )
        else:
            kfp_pip_install_command = make_pip_install_command(
                install_parts=[
                    f'kfp=={kfp.__version__}',
                    '--no-deps',
                    'typing-extensions>=3.7.4,<5; python_version<"3.9"',
                ],
                index_url_options=index_url_options,
            )
        pip_install_strings.append(kfp_pip_install_command)

    return [
        'sh', '-c',
        _install_python_packages_script_template.format(
            pip_install_commands=' '.join(pip_install_strings))
    ]


def _get_function_source_definition(func: Callable) -> str:
    func_code = inspect.getsource(func)

    # Function might be defined in some indented scope (e.g. in another
    # function). We need to handle this and properly dedent the function source
    # code
    func_code = textwrap.dedent(func_code)
    func_code_lines = func_code.split('\n')

    # Removing possible decorators (can be multiline) until the function
    # definition is found
    func_code_lines = itertools.dropwhile(lambda x: not x.startswith('def'),
                                          func_code_lines)

    if not func_code_lines:
        raise ValueError(
            f'Failed to dedent and clean up the source of function "{func.__name__}". It is probably not properly indented.'
        )

    return '\n'.join(func_code_lines)


def maybe_make_unique(name: str, names: List[str]):
    if name not in names:
        return name

    for i in range(2, 100):
        unique_name = f'{name}_{i}'
        if unique_name not in names:
            return unique_name

    raise RuntimeError(f'Too many arguments with the name {name}')


def get_name_to_specs(
    signature: inspect.Signature,
    containerized: bool = False,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Returns two dictionaries.

    The first is a mapping of input name to input annotation. The second
    is a mapping of output name to output annotation.
    """
    func_params = list(signature.parameters.values())

    name_to_input_specs = {}
    name_to_output_specs = {}

    ### handle function parameter annotations ###
    for func_param in func_params:
        name = func_param.name
        if name == SINGLE_OUTPUT_NAME:
            raise ValueError(
                f'"{SINGLE_OUTPUT_NAME}" is an invalid parameter name.')
        # Stripping Optional from Optional[<type>] is the only processing done
        # on annotations in this flow. Other than that, we extract the raw
        # annotation and process later.
        annotation = type_annotations.maybe_strip_optional_from_annotation(
            func_param.annotation)

        # Skip runtime-only bundled inputs. They are injected by the executor
        # and should not appear in the component interface.
        if isinstance(annotation, type_annotations.BundledInput):
            continue

        # no annotation
        if annotation == inspect._empty:
            raise TypeError(f'Missing type annotation for argument: {name}')

        # is Input[Artifact], Input[List[<Artifact>]], <param> (e.g., str), or InputPath(<param>)
        elif (type_annotations.is_artifact_wrapped_in_Input(annotation) or
              isinstance(
                  annotation,
                  type_annotations.InputPath,
              ) or type_utils.is_parameter_type(annotation)):
            name_to_input_specs[maybe_make_unique(
                name, list(name_to_input_specs))] = make_input_spec(
                    annotation, func_param)
        # is Artifact annotation (e.g., Artifact, Dataset, etc.)
        # or List[<Artifact>]
        elif type_annotations.issubclass_of_artifact(
                annotation) or type_annotations.is_list_of_artifacts(
                    annotation):
            if containerized:
                raise TypeError(
                    f"Container Components must wrap input and output artifact annotations with Input/Output type markers (Input[<artifact>] or Output[<artifact>]). Got function input '{name}' with annotation {annotation}."
                )
            name_to_input_specs[maybe_make_unique(
                name, list(name_to_input_specs))] = make_input_spec(
                    annotation, func_param)

        # is Output[Artifact] or OutputPath(<param>)
        elif type_annotations.is_artifact_wrapped_in_Output(
                annotation) or isinstance(annotation,
                                          type_annotations.OutputPath):
            name_to_output_specs[maybe_make_unique(
                name,
                list(name_to_output_specs))] = make_output_spec(annotation)

        # parameter type
        else:
            type_string = type_utils._annotation_to_type_struct(annotation)
            name_to_input_specs[maybe_make_unique(
                name, list(name_to_input_specs))] = make_input_spec(
                    type_string, func_param)

    ### handle return annotations ###
    return_ann = signature.return_annotation

    # validate container component returns
    if containerized:
        if return_ann not in [
                inspect.Parameter.empty,
                structures.ContainerSpec,
        ]:
            raise TypeError(
                'Return annotation should be either ContainerSpec or omitted for container components.'
            )
    # ignore omitted returns
    elif return_ann is None or return_ann == inspect.Parameter.empty:
        pass
    # is NamedTuple
    elif hasattr(return_ann, '_fields'):
        # Getting field type annotations.
        # __annotations__ does not exist in python 3.5 and earlier
        # _field_types does not exist in python 3.9 and later
        field_annotations = getattr(return_ann, '__annotations__',
                                    None) or getattr(return_ann, '_field_types')
        for name in return_ann._fields:
            annotation = field_annotations[name]
            if not type_annotations.is_list_of_artifacts(
                    annotation) and not type_annotations.is_artifact_class(
                        annotation):
                annotation = type_utils._annotation_to_type_struct(annotation)
            name_to_output_specs[maybe_make_unique(
                name,
                list(name_to_output_specs))] = make_output_spec(annotation)
    # is deprecated dict returns style
    elif isinstance(return_ann, dict):
        warnings.warn(
            'The ability to specify multiple outputs using the dict syntax'
            ' has been deprecated. It will be removed soon after release'
            ' 0.1.32. Please use typing.NamedTuple to declare multiple'
            ' outputs.', DeprecationWarning)
        for output_name, output_type_annotation in return_ann.items():
            output_type = type_utils._annotation_to_type_struct(
                output_type_annotation)
            name_to_output_specs[maybe_make_unique(
                output_name, list(name_to_output_specs))] = output_type
    # is the simple single return case (can be `-> <param>` or `-> Artifact`)
    # treated the same way, since processing is done in inner functions
    else:
        name_to_output_specs[maybe_make_unique(
            SINGLE_OUTPUT_NAME,
            list(name_to_output_specs))] = make_output_spec(return_ann)
    return name_to_input_specs, name_to_output_specs


def canonicalize_annotation(annotation: Any):
    """Does cleaning on annotations that are common between input and output
    annotations."""
    if type_annotations.is_Input_Output_artifact_annotation(annotation):
        annotation = type_annotations.strip_Input_or_Output_marker(annotation)
    if isinstance(annotation,
                  (type_annotations.InputPath, type_annotations.OutputPath)):
        annotation = annotation.type
    return annotation


def make_input_output_spec_args(annotation: Any) -> Dict[str, Any]:
    """Gets a dict of kwargs shared between InputSpec and OutputSpec."""
    is_artifact_list = type_annotations.is_list_of_artifacts(annotation)
    if is_artifact_list:
        annotation = type_annotations.get_inner_type(annotation)

    if type_annotations.issubclass_of_artifact(annotation):
        typ = type_utils.create_bundled_artifact_type(annotation.schema_title,
                                                      annotation.schema_version)
    else:
        typ = type_utils._annotation_to_type_struct(annotation)
    return {'type': typ, 'is_artifact_list': is_artifact_list}


def make_output_spec(annotation: Any) -> structures.OutputSpec:
    annotation = canonicalize_annotation(annotation)
    args = make_input_output_spec_args(annotation)
    return structures.OutputSpec(**args)


def make_input_spec(annotation: Any,
                    inspect_param: inspect.Parameter) -> structures.InputSpec:
    """Makes an InputSpec from a cleaned output annotation."""
    annotation = canonicalize_annotation(annotation)
    input_output_spec_args = make_input_output_spec_args(annotation)

    if (type_annotations.issubclass_of_artifact(annotation) or
            input_output_spec_args['is_artifact_list']
       ) and inspect_param.default not in {None, inspect._empty}:
        raise ValueError(
            f'Optional Input artifacts may only have default value None. Got: {inspect_param.default}.'
        )

    default = None if inspect_param.default == inspect.Parameter.empty or type_annotations.issubclass_of_artifact(
        annotation) else inspect_param.default

    optional = (
        inspect_param.default is not inspect.Parameter.empty or
        type_utils.is_task_final_status_type(
            getattr(inspect_param.annotation, '__name__', '')) or
        type_utils.is_task_config_type(
            getattr(inspect_param.annotation, '__name__', '')))
    return structures.InputSpec(
        **input_output_spec_args,
        default=default,
        optional=optional,
    )


def extract_component_interface(
    func: Callable,
    containerized: bool = False,
    description: Optional[str] = None,
    name: Optional[str] = None,
) -> structures.ComponentSpec:

    def assign_descriptions(
        inputs_or_outputs: Mapping[str, Union[structures.InputSpec,
                                              structures.OutputSpec]],
        docstring_params: List[docstring_parser.DocstringParam],
    ) -> None:
        """Assigns descriptions to InputSpec or OutputSpec for each component
        input/output found in the parsed docstring parameters."""
        docstring_inputs = {param.arg_name: param for param in docstring_params}
        for name, spec in inputs_or_outputs.items():
            if name in docstring_inputs:
                spec.description = docstring_inputs[name].description

    def parse_docstring_with_return_as_args(
            docstring: Union[str,
                             None]) -> Optional[docstring_parser.Docstring]:
        """Modifies docstring so that a return section can be treated as an
        args section, then parses the docstring."""
        if docstring is None:
            return None

        # Returns and Return are the only two keywords docstring_parser uses for returns
        # use newline to avoid replacements that aren't in the return section header
        return_keywords = ['Returns:\n', 'Returns\n', 'Return:\n', 'Return\n']
        for keyword in return_keywords:
            if keyword in docstring:
                modified_docstring = docstring.replace(keyword.strip(), 'Args:')
                return docstring_parser.parse(modified_docstring)

        return None

    signature = inspect.signature(func)
    name_to_input_spec, name_to_output_spec = get_name_to_specs(
        signature, containerized)
    original_docstring = inspect.getdoc(func)
    parsed_docstring = docstring_parser.parse(original_docstring)

    assign_descriptions(name_to_input_spec, parsed_docstring.params)

    modified_parsed_docstring = parse_docstring_with_return_as_args(
        original_docstring)
    if modified_parsed_docstring is not None:
        assign_descriptions(name_to_output_spec,
                            modified_parsed_docstring.params)

    description = get_pipeline_description(
        decorator_description=description,
        docstring=parsed_docstring,
    )

    component_name = name or _python_function_name_to_component_name(
        func.__name__)
    return structures.ComponentSpec(
        name=component_name,
        description=description,
        inputs=name_to_input_spec or None,
        outputs=name_to_output_spec or None,
        implementation=structures.Implementation(),
    )


EXECUTOR_MODULE = 'kfp.dsl.executor_main'
CONTAINERIZED_PYTHON_COMPONENT_COMMAND = [
    'python3',
    '-m',
    EXECUTOR_MODULE,
]


def _get_command_and_args_for_lightweight_component(
    func: Callable,
    additional_funcs: Optional[List[Callable]] = None
) -> Tuple[List[str], List[str]]:
    imports_source = [
        'import kfp',
        'from kfp import dsl',
        'from kfp.dsl import *',
        'from typing import *',
    ] + custom_artifact_types.get_custom_artifact_type_import_statements(func)

    func_source = _get_function_source_definition(func)
    additional_funcs_source = ''
    if additional_funcs:
        additional_funcs_source = '\n\n' + '\n\n'.join(
            _get_function_source_definition(_f) for _f in additional_funcs)
    source = textwrap.dedent('''
        {imports_source}{additional_funcs_source}

        {func_source}\n''').format(
        imports_source='\n'.join(imports_source),
        additional_funcs_source=additional_funcs_source,
        func_source=func_source)
    command = [
        'sh',
        '-ec',
        textwrap.dedent(f'''\
                    program_path=$(mktemp -d)

                    printf "%s" "$0" > "$program_path/ephemeral_component.py"
                    _KFP_RUNTIME=true python3 -m {EXECUTOR_MODULE} \
                        --component_module_path \
                        "$program_path/ephemeral_component.py" \
                        "$@"
                '''),
        source,
    ]

    args = [
        '--executor_input',
        dsl.PIPELINE_TASK_EXECUTOR_INPUT_PLACEHOLDER,
        '--function_to_execute',
        func.__name__,
    ]

    return command, args


def _resolve_effective_packages(packages_to_install: Optional[List[str]],
                                defaults: List[str]) -> List[str]:
    if packages_to_install is None:
        return list(defaults)
    return list(packages_to_install)


def _resolve_base_image(base_image: Optional[str], decorator_name: str) -> str:
    if base_image is None:
        base_image = _DEFAULT_BASE_IMAGE
        warnings.warn(
            (f"The default base_image used by {decorator_name} will switch from 'python:3.9' to 'python:3.10' on Oct 1, 2025. "
             "To ensure your existing components work with versions of the KFP SDK released after that date, you should provide an explicit base_image argument and ensure your component works as intended on Python 3.10."),
            FutureWarning,
            stacklevel=2,
        )
    return base_image


def _embed_file_to_b64(asset_path: pathlib.Path) -> str:
    import base64
    return base64.b64encode(asset_path.read_bytes()).decode('ascii')


def _build_ephemeral_source(import_lines: List[str], helper_source: str,
                            func: Callable) -> str:
    """Builds the ephemeral module source without using str.format to avoid brace collisions.

    Concatenates import lines, helper source, and the user function source.
    """
    func_source = _get_function_source_definition(func)
    parts = [
        '\n'.join(import_lines),
        '',
        helper_source,
        '',
        func_source,
        '',
    ]
    return '\n'.join(parts)


def _get_command_and_args_for_ephemeral_source(
        func: Callable, source: str) -> Tuple[List[str], List[str]]:
    command = [
        'sh',
        '-ec',
        textwrap.dedent(f'''\
                    program_path=$(mktemp -d)

                    printf "%s" "$0" > "$program_path/ephemeral_component.py"
                    _KFP_RUNTIME=true python3 -m {EXECUTOR_MODULE} \
                        --component_module_path \
                        "$program_path/ephemeral_component.py" \
                        "$@"
                '''),
        source,
    ]
    args = [
        '--executor_input',
        dsl.PIPELINE_TASK_EXECUTOR_INPUT_PLACEHOLDER,
        '--function_to_execute',
        func.__name__,
    ]
    return command, args


def _generate_notebook_helper_source(embedded_json_placeholder: str) -> str:
    """Generate the notebook execution helper source code from Python template.

    Uses a Python module to generate the template, which is more robust
    than file-based templates since it's properly packaged and importable.
    """
    from kfp.dsl.templates.notebook_executor import get_notebook_executor_source
    return get_notebook_executor_source(embedded_json_placeholder)


def _generate_bundle_helper_source(bundle_tgz_b64: str, root_name: str) -> str:
    """Generate helper source that extracts a base64 tar.gz bundle at import time.

    Defines a module-global `__KFP_BUNDLE_DIR` that points to the extracted root.
    """
    # Keep this small to avoid command-size growth; do safety checks for path traversal.
    return f"""__KFP_BUNDLE_TGZ_B64 = '{bundle_tgz_b64}'
__KFP_BUNDLE_ROOT_NAME = '{root_name}'

import base64 as __kfp_b64
import tarfile as __kfp_tarfile
import io as __kfp_io
import tempfile as __kfp_tempfile
import os as __kfp_os

def __kfp__is_within_directory(directory, target):
    abs_directory = __kfp_os.path.abspath(directory)
    abs_target = __kfp_os.path.abspath(target)
    return __kfp_os.path.commonprefix([abs_directory, abs_target]) == abs_directory

def __kfp__extract_bundle() -> str:
    tmpdir = __kfp_tempfile.mkdtemp()
    data = __kfp_b64.b64decode(__KFP_BUNDLE_TGZ_B64.encode('ascii'))
    with __kfp_tarfile.open(fileobj=__kfp_io.BytesIO(data), mode='r:gz') as tar:
        for m in tar.getmembers():
            target = __kfp_os.path.join(tmpdir, m.name)
            if not __kfp__is_within_directory(tmpdir, target):
                raise Exception('Attempted Path Traversal in Tar File')
        tar.extractall(tmpdir)
    return tmpdir

__KFP_BUNDLE_DIR = __kfp__extract_bundle()
__KFP_BUNDLE_PATH = __kfp_os.path.join(__KFP_BUNDLE_DIR, __KFP_BUNDLE_ROOT_NAME)
"""


def _build_ephemeral_source_with_helper(import_lines: List[str], helper_source: str,
                                        func: Callable) -> str:
    """Builds ephemeral module source with a provided helper.

    This avoids brace collisions by concatenation and mirrors _build_ephemeral_source.
    """
    func_source = _get_function_source_definition(func)
    parts = [
        '\n'.join(import_lines),
        '',
        helper_source,
        '',
        func_source,
        '',
    ]
    return '\n'.join(parts)


def _get_command_and_args_for_asset_backed_component(
    func: Callable,
    *,
    asset_json_compressed: str,
) -> Tuple[List[str], List[str]]:
    """Builds command/args for a component that embeds an asset and uses Python executor.

    Currently the asset is a notebook that is executed via nbconvert when
    `dsl.run_notebook(**kwargs)` is called. The design is generic so the
    embedding mechanism can be reused for other Python assets in the future.
    """
    import_lines = [
        'import kfp',
        'from kfp import dsl',
        'from kfp.dsl import *',
        'from typing import *',
        'import json, os, tempfile',
        'import nbformat',
        'from nbconvert.preprocessors import ExecutePreprocessor',
    ] + custom_artifact_types.get_custom_artifact_type_import_statements(func)

    helper_source = _generate_notebook_helper_source(asset_json_compressed)
    source = _build_ephemeral_source(
        import_lines=import_lines, helper_source=helper_source, func=func)
    return _get_command_and_args_for_ephemeral_source(func=func, source=source)


def _get_command_and_args_for_bundled_component(
    func: Callable,
    *,
    bundle_tgz_b64: str,
    bundle_root_name: str,
) -> Tuple[List[str], List[str]]:
    """Builds command/args for a component that embeds a tar.gz bundle.

    The bundle is extracted at import time and exposed via __KFP_BUNDLE_DIR.
    """
    import_lines = [
        'import kfp',
        'from kfp import dsl',
        'from kfp.dsl import *',
        'from typing import *',
    ] + custom_artifact_types.get_custom_artifact_type_import_statements(func)

    helper_source = _generate_bundle_helper_source(bundle_tgz_b64, bundle_root_name)
    source = _build_ephemeral_source_with_helper(
        import_lines=import_lines, helper_source=helper_source, func=func)
    return _get_command_and_args_for_ephemeral_source(func=func, source=source)


def create_notebook_component_from_func(
    func: Callable,
    *,
    notebook_path: str,
    base_image: Optional[str] = None,
    packages_to_install: Optional[List[str]] = None,
    pip_index_urls: Optional[List[str]] = None,
    output_component_file: Optional[str] = None,
    install_kfp_package: bool = True,
    kfp_package_path: Optional[str] = None,
    pip_trusted_hosts: Optional[List[str]] = None,
    use_venv: bool = False,
    task_config_passthroughs: Optional[List[TaskConfigPassthrough]] = None,
) -> python_component.PythonComponent:
    """Builds a notebook-based component from a Python function signature.

    Always embeds the notebook bytes and uses nbconvert at runtime.
    """
    # packages behavior: None -> default nb deps; [] -> none; non-empty -> exact
    # Default to ranges that avoid major version bumps.
    # - jupyter>=1,<2 is a stable meta-package range
    # - nbconvert>=7,<8 aligns with current stable major
    effective_packages: List[str] = _resolve_effective_packages(
        packages_to_install, defaults=['jupyter>=1,<2', 'nbconvert>=7,<8'])

    packages_to_install_command = _get_packages_to_install_command(
        install_kfp_package=install_kfp_package,
        target_image=None,
        kfp_package_path=kfp_package_path,
        packages_to_install=effective_packages,
        pip_index_urls=pip_index_urls,
        pip_trusted_hosts=pip_trusted_hosts,
        use_venv=use_venv,
    )

    base_image = _resolve_base_image(
        base_image, decorator_name='@dsl.notebook_component')

    # Read and embed notebook JSON
    nb_path = pathlib.Path(notebook_path)
    if not nb_path.exists() or nb_path.suffix.lower() != '.ipynb':
        raise ValueError(f'Invalid notebook_path: {notebook_path}')
    notebook_json_text = nb_path.read_text(encoding='utf-8')
    # Validate JSON and normalize formatting for embedding
    try:
        nb_obj = json.loads(notebook_json_text)
    except json.JSONDecodeError as e:
        raise ValueError(
            f'Notebook "{notebook_path}" is not valid JSON: {e.msg} '
            f'at line {e.lineno}, column {e.colno}.') from e
    if not isinstance(nb_obj, dict) or 'cells' not in nb_obj:
        raise ValueError(
            f'Notebook "{notebook_path}" JSON root must be an object with a "cells" field. '
            f'Got type {type(nb_obj).__name__}.')

    # Compact re-serialization to reduce command length
    notebook_json_text = json.dumps(nb_obj, separators=(',', ':'))
    
    # Compress the notebook JSON to further reduce command length
    import base64
    import gzip
    notebook_json_bytes = notebook_json_text.encode('utf-8')
    compressed_bytes = gzip.compress(notebook_json_bytes, compresslevel=9)
    notebook_json_compressed = base64.b64encode(compressed_bytes).decode('ascii')

    # Report compression statistics and warn about large notebooks
    original_size_mb = len(notebook_json_bytes) / (1024 * 1024)
    compressed_size_mb = len(compressed_bytes) / (1024 * 1024)
    compression_ratio = len(compressed_bytes) / len(notebook_json_bytes) if notebook_json_bytes else 0
    
    if original_size_mb > 1:  # Lower threshold since we now compress
        warnings.warn(
            f"Notebook {notebook_path} is large ({original_size_mb:.1f}MB original, "
            f"{compressed_size_mb:.1f}MB compressed, {compression_ratio:.1%} ratio). "
            f"This may cause issues with component execution due to large command size.",
            UserWarning,
            stacklevel=2)

    command, args = _get_command_and_args_for_asset_backed_component(
        func=func,
        asset_json_compressed=notebook_json_compressed,
    )

    component_spec = extract_component_interface(func)
    if task_config_passthroughs:
        component_spec.task_config_passthroughs = task_config_passthroughs
    component_spec.implementation = structures.Implementation(
        container=structures.ContainerSpecImplementation(
            image=base_image,
            command=packages_to_install_command + command,
            args=args,
        ))

    module_path = pathlib.Path(inspect.getsourcefile(func))
    module_path.resolve()
    component_name = _python_function_name_to_component_name(func.__name__)
    component_info = ComponentInfo(
        name=component_name,
        function_name=func.__name__,
        func=func,
        target_image=None,
        module_path=module_path,
        component_spec=component_spec,
        output_component_file=output_component_file,
        base_image=base_image,
        packages_to_install=effective_packages,
        pip_index_urls=pip_index_urls,
        pip_trusted_hosts=pip_trusted_hosts,
        task_config_passthroughs=task_config_passthroughs,
    )
    if REGISTERED_MODULES is not None:
        REGISTERED_MODULES[component_name] = component_info

    if output_component_file:
        component_spec.save_to_component_yaml(output_component_file)

    return python_component.PythonComponent(
        component_spec=component_spec, python_func=func)


def _get_command_and_args_for_containerized_component(
        function_name: str) -> Tuple[List[str], List[str]]:

    args = [
        '--executor_input',
        dsl.PIPELINE_TASK_EXECUTOR_INPUT_PLACEHOLDER,
        '--function_to_execute',
        function_name,
    ]
    return CONTAINERIZED_PYTHON_COMPONENT_COMMAND, args


def create_component_from_func(
    func: Callable,
    base_image: Optional[str] = None,
    target_image: Optional[str] = None,
    bundled_artifact_path: Optional[str] = None,
    packages_to_install: List[str] = None,
    pip_index_urls: Optional[List[str]] = None,
    output_component_file: Optional[str] = None,
    install_kfp_package: bool = True,
    kfp_package_path: Optional[str] = None,
    pip_trusted_hosts: Optional[List[str]] = None,
    use_venv: bool = False,
    additional_funcs: Optional[List[Callable]] = None,
    task_config_passthroughs: Optional[List[TaskConfigPassthrough]] = None,
) -> python_component.PythonComponent:
    """Implementation for the @component decorator.

    The decorator is defined under component_decorator.py. See the
    decorator for the canonical documentation for this function.
    """

    packages_to_install_command = _get_packages_to_install_command(
        install_kfp_package=install_kfp_package,
        target_image=target_image,
        kfp_package_path=kfp_package_path,
        packages_to_install=packages_to_install,
        pip_index_urls=pip_index_urls,
        pip_trusted_hosts=pip_trusted_hosts,
        use_venv=use_venv,
    )

    command = []
    args = []
    if base_image is None:
        base_image = _DEFAULT_BASE_IMAGE
        warnings.warn(
            ("The default base_image used by the @dsl.component decorator will switch from 'python:3.9' to 'python:3.10' on Oct 1, 2025. To ensure your existing components work with versions of the KFP SDK released after that date, you should provide an explicit base_image argument and ensure your component works as intended on Python 3.10."
            ),
            FutureWarning,
            stacklevel=2,
        )

    component_image = base_image

    if target_image:
        component_image = target_image
        command, args = _get_command_and_args_for_containerized_component(
            function_name=func.__name__,)
    else:
        if bundled_artifact_path:
            # Build tar.gz in memory and base64-encode
            import io, tarfile, base64, pathlib as _pl
            p = _pl.Path(bundled_artifact_path)
            if not p.exists():
                raise ValueError(f'bundled_artifact_path does not exist: {bundled_artifact_path}')
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode='w:gz') as tar:
                if p.is_dir():
                    # When directory, we add contents under a stable root name
                    root_name = '.'
                    for child in p.iterdir():
                        tar.add(str(child), arcname=child.name)
                else:
                    # Single file: add with its basename
                    root_name = p.name
                    tar.add(str(p), arcname=p.name)
            bundle_b64 = base64.b64encode(buf.getvalue()).decode('ascii')
            command, args = _get_command_and_args_for_bundled_component(
                func=func, bundle_tgz_b64=bundle_b64, bundle_root_name=root_name)
        else:
            command, args = _get_command_and_args_for_lightweight_component(
                func=func, additional_funcs=additional_funcs)

    component_spec = extract_component_interface(func)
    # Attach task_config_passthroughs to the ComponentSpec structure if provided.
    if task_config_passthroughs:
        component_spec.task_config_passthroughs = task_config_passthroughs
    component_spec.implementation = structures.Implementation(
        container=structures.ContainerSpecImplementation(
            image=component_image,
            command=packages_to_install_command + command,
            args=args,
        ))

    module_path = pathlib.Path(inspect.getsourcefile(func))
    module_path.resolve()

    component_name = _python_function_name_to_component_name(func.__name__)
    component_info = ComponentInfo(
        name=component_name,
        function_name=func.__name__,
        func=func,
        target_image=target_image,
        module_path=module_path,
        component_spec=component_spec,
        output_component_file=output_component_file,
        base_image=base_image,
        packages_to_install=packages_to_install,
        pip_index_urls=pip_index_urls,
        pip_trusted_hosts=pip_trusted_hosts,
        task_config_passthroughs=task_config_passthroughs)

    if REGISTERED_MODULES is not None:
        REGISTERED_MODULES[component_name] = component_info

    if output_component_file:
        component_spec.save_to_component_yaml(output_component_file)

    return python_component.PythonComponent(
        component_spec=component_spec, python_func=func)


def make_input_for_parameterized_container_component_function(
    name: str, annotation: Union[Type[List[artifact_types.Artifact]],
                                 Type[artifact_types.Artifact]]
) -> Union[placeholders.Placeholder, container_component_artifact_channel
           .ContainerComponentArtifactChannel]:
    if type_annotations.is_artifact_wrapped_in_Input(annotation):

        if type_annotations.is_list_of_artifacts(annotation.__origin__):
            return placeholders.InputListOfArtifactsPlaceholder(name)
        else:
            return container_component_artifact_channel.ContainerComponentArtifactChannel(
                io_type='input', var_name=name)

    elif type_annotations.is_artifact_wrapped_in_Output(annotation):

        if type_annotations.is_list_of_artifacts(annotation.__origin__):
            return placeholders.OutputListOfArtifactsPlaceholder(name)
        else:
            return container_component_artifact_channel.ContainerComponentArtifactChannel(
                io_type='output', var_name=name)

    elif isinstance(
            annotation,
        (type_annotations.OutputAnnotation, type_annotations.OutputPath)):
        return placeholders.OutputParameterPlaceholder(name)

    else:
        placeholder = placeholders.InputValuePlaceholder(name)
        # small hack to encode the runtime value's type for a custom json.dumps function
        if (annotation == task_final_status.PipelineTaskFinalStatus or
                type_utils.is_task_final_status_type(annotation)):
            placeholder._ir_type = 'STRUCT'
        elif annotation == TaskConfig:
            # Treat as STRUCT for IR and use reserved input name at runtime.
            placeholder._ir_type = 'STRUCT'
        else:
            placeholder._ir_type = type_utils.get_parameter_type_name(
                annotation)
        return placeholder


def create_container_component_from_func(
        func: Callable) -> container_component_class.ContainerComponent:
    """Implementation for the @container_component decorator.

    The decorator is defined under container_component_decorator.py. See
    the decorator for the canonical documentation for this function.
    """

    component_spec = extract_component_interface(func, containerized=True)
    signature = inspect.signature(func)
    parameters = list(signature.parameters.values())
    arg_list = []
    for parameter in parameters:
        parameter_type = type_annotations.maybe_strip_optional_from_annotation(
            parameter.annotation)
        arg_list.append(
            make_input_for_parameterized_container_component_function(
                parameter.name, parameter_type))

    container_spec = func(*arg_list)
    container_spec_implementation = structures.ContainerSpecImplementation.from_container_spec(
        container_spec)
    component_spec.implementation = structures.Implementation(
        container_spec_implementation)
    component_spec._validate_placeholders()
    return container_component_class.ContainerComponent(component_spec, func)


def create_graph_component_from_func(
    func: Callable,
    name: Optional[str] = None,
    description: Optional[str] = None,
    display_name: Optional[str] = None,
    pipeline_config: pipeline_config.PipelineConfig = None,
) -> graph_component.GraphComponent:
    """Implementation for the @pipeline decorator.

    The decorator is defined under pipeline_context.py. See the
    decorator for the canonical documentation for this function.
    """

    component_spec = extract_component_interface(
        func,
        description=description,
        name=name,
    )
    return graph_component.GraphComponent(
        component_spec=component_spec,
        pipeline_func=func,
        display_name=display_name,
        pipeline_config=pipeline_config,
    )


def get_pipeline_description(
    decorator_description: Union[str, None],
    docstring: docstring_parser.Docstring,
) -> Optional[str]:
    """Obtains the correct pipeline description from the pipeline decorator's
    description argument and the parsed docstring.

    Gives precedence to the decorator argument.
    """
    if decorator_description:
        return decorator_description

    short_description = docstring.short_description
    long_description = docstring.long_description
    docstring_description = short_description + '\n' + long_description if (
        short_description and long_description) else short_description
    return docstring_description.strip() if docstring_description else None
