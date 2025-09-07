"""Decorator for creating notebook-based components.

Uses the existing Python executor. The embedded notebook is executed by a
helper bound to `dsl.run_notebook(**kwargs)` that users can call inside their
component function.
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional

from kfp.dsl import component_factory
from kfp.dsl.component_task_config import TaskConfigPassthrough, TaskConfigField


def notebook_component(
    func: Optional[Callable[..., Any]] = None,
    *,
    notebook_path: str,
    base_image: Optional[str] = None,
    packages_to_install: Optional[List[str]] = None,
    output_component_file: Optional[str] = None,
    pip_index_urls: Optional[List[str]] = None,
    pip_trusted_hosts: Optional[List[str]] = None,
    use_venv: bool = False,
    kfp_package_path: Optional[str] = None,
    install_kfp_package: bool = True,
    task_config_passthroughs: Optional[List[TaskConfigPassthrough]] = None,
):
    """Decorator to define a Notebook-based KFP component.

    Args:
        notebook_path: Path to the .ipynb file to embed and execute.
        base_image: Base container image for the component.
        packages_to_install: See behavior rules below. When None, defaults to
            ["jupyter>=1,<2", "nbconvert>=7,<8"] to avoid major version bumps.
            When [], installs nothing. When non-empty, installs the exact list.
        output_component_file: Optional path to write the component YAML.
        pip_index_urls: Optional pip index URLs for installation.
        pip_trusted_hosts: Optional pip trusted hosts.
        use_venv: Whether to create and use a venv inside the container.
        kfp_package_path: Optional KFP package path to install.
        install_kfp_package: Whether to auto-install KFP when appropriate.
        task_config_passthroughs: Optional task config passthroughs.
    """

    def wrapper(user_func: Callable[..., Any]):
        nonlocal task_config_passthroughs
        task_config_passthroughs_formatted: Optional[
            List[TaskConfigPassthrough]] = None
        if task_config_passthroughs is not None:
            task_config_passthroughs_formatted = []
            for passthrough in task_config_passthroughs:
                if isinstance(passthrough, TaskConfigField):
                    task_config_passthroughs_formatted.append(
                        TaskConfigPassthrough(
                            field=passthrough, apply_to_task=False))
                else:
                    task_config_passthroughs_formatted.append(passthrough)

        return component_factory.create_notebook_component_from_func(
            func=user_func,
            notebook_path=notebook_path,
            base_image=base_image,
            packages_to_install=packages_to_install,
            output_component_file=output_component_file,
            pip_index_urls=pip_index_urls,
            pip_trusted_hosts=pip_trusted_hosts,
            use_venv=use_venv,
            kfp_package_path=kfp_package_path,
            install_kfp_package=install_kfp_package,
            task_config_passthroughs=task_config_passthroughs_formatted,
        )

    if func is None:
        return wrapper
    else:
        return wrapper(func)
