"""Template for notebook executor code generation.

This module contains the actual Python functions that get embedded into notebook 
components at runtime. Using inspect.getsource() to convert them to source code
provides better maintainability, testing, and IDE support.

Note: The template functions import their dependencies locally to avoid requiring
those dependencies when this module is imported for code generation.
"""

import inspect
import textwrap


def __kfp__write_parameters_cell(nb, params):
    """Inject parameters as a code cell at the beginning of the notebook."""
    import json
    import nbformat

    if not params:
        return
    assignments = []
    for key, value in params.items():
        serialized = json.dumps(value)
        assignments.append(key + ' = json.loads(' + repr(serialized) + ')')
    source = 'import json\n' + '\n'.join(assignments) + '\n'
    cell = nbformat.v4.new_code_cell(source=source)
    nb.cells.insert(0, cell)


def __kfp__stream_single_output(output, cell_idx):
    """Handle streaming for a single notebook output."""
    import sys
    output_type = output.get('output_type')

    if output_type == 'stream':
        text = output.get('text', '')
        if text:
            try:
                print(f'[nb cell {cell_idx} stream] ', end='', flush=False)
            except Exception:
                pass
            print(text, end='' if text.endswith('\n') else '\n', flush=True)
    elif output_type == 'error':
        for line in output.get('traceback', []):
            print(line, file=sys.stderr, flush=True)
    else:
        # Handle display_data and execute_result
        data = output.get('data', {})
        if 'text/plain' in data:
            print(data['text/plain'], flush=True)


def __kfp__stream_notebook_outputs(nb):
    """Stream all outputs from executed notebook cells."""
    for idx, cell in enumerate(nb.get('cells', [])):
        for output in cell.get('outputs', []):
            __kfp__stream_single_output(output, idx)


def __kfp__run_notebook(**kwargs):
    """Execute the embedded notebook with injected parameters."""
    import tempfile
    import nbformat
    from nbconvert.preprocessors import ExecutePreprocessor
    import base64
    import gzip

    # Decompress the embedded notebook JSON
    compressed_bytes = base64.b64decode(__KFP_EMBEDDED_NOTEBOOK_JSON.encode('ascii'))
    notebook_json_text = gzip.decompress(compressed_bytes).decode('utf-8')
    nb = nbformat.reads(notebook_json_text, as_version=4)
    with tempfile.TemporaryDirectory() as tmpdir:
        __kfp__write_parameters_cell(nb, kwargs)
        ep = ExecutePreprocessor(allow_errors=False, store_widget_state=True)
        print(
            '[KFP Notebook] Executing embedded notebook with',
            len(nb.get('cells', [])),
            'cells',
            flush=True)
        ep.preprocess(nb, {'metadata': {'path': tmpdir}})
        print(
            '[KFP Notebook] Execution complete; streaming cell outputs:',
            flush=True)
        __kfp__stream_notebook_outputs(nb)


def get_notebook_executor_source(embedded_json_placeholder: str) -> str:
    """Generate the notebook execution helper source code.
    
    Uses inspect.getsource() to extract the actual function definitions,
    providing better maintainability than string templates.
    
    Args:
        embedded_json_placeholder: The JSON-encoded notebook content as a string
        
    Returns:
        Python source code for notebook execution helpers
    """
    # Get source code for all the helper functions
    functions = [
        __kfp__write_parameters_cell, __kfp__stream_single_output,
        __kfp__stream_notebook_outputs, __kfp__run_notebook
    ]

    # Extract and dedent source code for all helper functions
    function_sources = [
        textwrap.dedent(inspect.getsource(func)) for func in functions
    ]

    # Combine everything into the final source
    functions_code = '\n'.join(function_sources)
    return f"""__KFP_EMBEDDED_NOTEBOOK_JSON = '{embedded_json_placeholder}'

{functions_code}

# Bind helper into dsl namespace so user code can call dsl.run_notebook(...)
dsl.run_notebook = __kfp__run_notebook"""
