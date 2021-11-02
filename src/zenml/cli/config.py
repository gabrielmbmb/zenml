#  Copyright (c) ZenML GmbH 2021. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""CLI for manipulating ZenML local and global config file."""
import time
from typing import TYPE_CHECKING, List, Optional, Tuple

import click

from zenml.cli import utils as cli_utils
from zenml.cli.cli import cli
from zenml.config.global_config import GlobalConfig
from zenml.core.component_factory import (
    artifact_store_factory,
    metadata_store_factory,
    orchestrator_store_factory,
)
from zenml.core.repo import Repository
from zenml.enums import LoggingLevels

if TYPE_CHECKING:
    from zenml.orchestrators.base_orchestrator import BaseOrchestrator


# Analytics
@cli.group()
def analytics() -> None:
    """Analytics for opt-in and opt-out"""


@analytics.command("opt-in", context_settings=dict(ignore_unknown_options=True))
def opt_in() -> None:
    """Opt-in to analytics"""
    gc = GlobalConfig()
    gc.analytics_opt_in = True
    gc.update()
    cli_utils.declare("Opted in to analytics.")


@analytics.command(
    "opt-out", context_settings=dict(ignore_unknown_options=True)
)
def opt_out() -> None:
    """Opt-out to analytics"""
    gc = GlobalConfig()
    gc.analytics_opt_in = False
    gc.update()
    cli_utils.declare("Opted out of analytics.")


# Logging
@cli.group()
def logging() -> None:
    """Configuration of logging for ZenML pipelines."""


# Setting logging
@logging.command("set-verbosity")
@click.argument(
    "verbosity",
    type=click.Choice(
        list(map(lambda x: x.name, LoggingLevels)), case_sensitive=False
    ),
)
def set_logging_verbosity(verbosity: str) -> None:
    """Set logging level"""
    verbosity = verbosity.upper()
    if verbosity not in LoggingLevels.__members__:
        raise KeyError(
            f"Verbosity must be one of {list(LoggingLevels.__members__.keys())}"
        )
    cli_utils.declare(f"Set verbosity to: {verbosity}")


# Metadata Store
@cli.group()
def metadata() -> None:
    """Utilities for metadata store"""


@metadata.command(
    "register", context_settings=dict(ignore_unknown_options=True)
)
@click.argument("metadata_store_name", type=str)
@click.argument("metadata_store_type", type=str)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def register_metadata_store(
    metadata_store_name: str, metadata_store_type: str, args: List[str]
) -> None:
    """Register a metadata store."""

    try:
        parsed_args = cli_utils.parse_unknown_options(args)
    except AssertionError as e:
        cli_utils.error(str(e))
        return

    repo: Repository = Repository()
    try:
        comp = metadata_store_factory.get_single_component(metadata_store_type)
    except AssertionError as e:
        cli_utils.error(str(e))
        return

    metadata_store = comp(**parsed_args)
    service = repo.get_service()
    service.register_metadata_store(metadata_store_name, metadata_store)
    cli_utils.declare(
        f"Metadata Store `{metadata_store_name}` successfully registered!"
    )


@metadata.command("list")
def list_metadata_stores() -> None:
    """List all available metadata stores from service."""
    service = Repository().get_service()
    cli_utils.title("Metadata Stores:")
    cli_utils.echo_component_list(service.metadata_stores)


@metadata.command("delete")
@click.argument("metadata_store_name", type=str)
def delete_metadata_store(metadata_store_name: str) -> None:
    """Delete a metadata store."""
    service = Repository().get_service()
    service.delete_metadata_store(metadata_store_name)
    cli_utils.declare(f"Deleted metadata store: {metadata_store_name}")


# Artifact Store
@cli.group()
def artifact() -> None:
    """Utilities for artifact store"""


@artifact.command(
    "register", context_settings=dict(ignore_unknown_options=True)
)
@click.argument("artifact_store_name", type=str)
@click.argument("artifact_store_type", type=str)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def register_artifact_store(
    artifact_store_name: str, artifact_store_type: str, args: List[str]
) -> None:
    """Register an artifact store."""

    try:
        parsed_args = cli_utils.parse_unknown_options(args)
    except AssertionError as e:
        cli_utils.error(str(e))
        return

    repo: Repository = Repository()
    comp = artifact_store_factory.get_single_component(artifact_store_type)
    artifact_store = comp(**parsed_args)
    service = repo.get_service()
    service.register_artifact_store(artifact_store_name, artifact_store)
    cli_utils.declare(
        f"Artifact Store `{artifact_store_name}` successfully registered!"
    )


@artifact.command("list")
def list_artifact_stores() -> None:
    """List all available artifact stores from service."""
    service = Repository().get_service()
    cli_utils.title("Artifact Stores:")
    cli_utils.echo_component_list(service.artifact_stores)


@artifact.command("delete")
@click.argument("artifact_store_name", type=str)
def delete_artifact_store(artifact_store_name: str) -> None:
    """Delete a artifact store."""
    service = Repository().get_service()
    service.delete_artifact_store(artifact_store_name)
    cli_utils.declare(f"Deleted artifact store: {artifact_store_name}")


# Orchestrator
@cli.group()
def orchestrator() -> None:
    """Utilities for orchestrator"""


@orchestrator.command(
    "register", context_settings=dict(ignore_unknown_options=True)
)
@click.argument("orchestrator_name", type=str)
@click.argument("orchestrator_type", type=str)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def register_orchestrator(
    orchestrator_name: str, orchestrator_type: str, args: List[str]
) -> None:
    """Register an orchestrator."""

    try:
        parsed_args = cli_utils.parse_unknown_options(args)
    except AssertionError as e:
        cli_utils.error(str(e))
        return

    repo: Repository = Repository()
    comp = orchestrator_store_factory.get_single_component(orchestrator_type)
    orchestrator_ = comp(**parsed_args)
    service = repo.get_service()
    service.register_orchestrator(orchestrator_name, orchestrator_)
    cli_utils.declare(
        f"Orchestrator `{orchestrator_name}` successfully registered!"
    )


@orchestrator.command("list")
def list_orchestrators() -> None:
    """List all available orchestrators from service."""
    service = Repository().get_service()
    cli_utils.title("Orchestrators:")
    cli_utils.echo_component_list(service.orchestrators)


@orchestrator.command("delete")
@click.argument("orchestrator_name", type=str)
def delete_orchestrator(orchestrator_name: str) -> None:
    """Delete a orchestrator."""
    service = Repository().get_service()
    service.delete_orchestrator(orchestrator_name)
    cli_utils.declare(f"Deleted orchestrator: `{orchestrator_name}`")


def _get_orchestrator(
    orchestrator_name: Optional[str] = None,
) -> Tuple["BaseOrchestrator", str]:
    """Gets an orchestrator for a given name.

    Args:
        orchestrator_name: Name of the orchestrator to get. If `None`, the
            orchestrator of the active stack gets returned.

    Returns:
        A tuple containing the orchestrator and its name.

    Raises:
        DoesNotExistException: If no orchestrator for the name exists.
    """
    if not orchestrator_name:
        active_stack = Repository().get_active_stack()
        orchestrator_name = active_stack.orchestrator_name
        cli_utils.declare(
            f"No orchestrator name given, using `{orchestrator_name}` "
            f"from active stack."
        )

    service = Repository().get_service()
    return service.get_orchestrator(orchestrator_name), orchestrator_name


@orchestrator.command("up")
@click.argument("orchestrator_name", type=str, required=False)
def up_orchestrator(orchestrator_name: Optional[str] = None) -> None:
    """Provisions resources for the orchestrator"""
    orchestrator_, orchestrator_name = _get_orchestrator(orchestrator_name)

    cli_utils.declare(
        f"Bootstrapping resources for orchestrator: `{orchestrator_name}`. "
        f"This might take a few seconds..."
    )
    orchestrator_.up()
    cli_utils.declare(f"Orchestrator: `{orchestrator_name}` is up.")


@orchestrator.command("down")
@click.argument("orchestrator_name", type=str, required=False)
def down_orchestrator(orchestrator_name: Optional[str] = None) -> None:
    """Tears down resources for the orchestrator"""
    orchestrator_, orchestrator_name = _get_orchestrator(orchestrator_name)

    cli_utils.declare(
        f"Tearing down resources for orchestrator: `{orchestrator_name}`."
    )
    orchestrator_.down()
    cli_utils.declare(
        f"Orchestrator: `{orchestrator_name}` resources are now torn down."
    )


@orchestrator.command("monitor")
@click.argument("orchestrator_name", type=str, required=False)
def monitor_orchestrator(orchestrator_name: Optional[str] = None) -> None:
    """Monitor a running orchestrator."""
    orchestrator_, orchestrator_name = _get_orchestrator(orchestrator_name)
    if not orchestrator_.is_running:
        cli_utils.warning(
            f"Can't monitor orchestrator '{orchestrator_name}' "
            f"because it isn't running."
        )
        return

    if not orchestrator_.log_file:
        cli_utils.warning(f"Can't monitor orchestrator '{orchestrator_name}'.")
        return

    cli_utils.declare(
        f"Monitoring orchestrator '{orchestrator_name}', press CTRL+C to stop."
    )
    try:
        with open(orchestrator_.log_file, "r") as log_file:
            # seek to the end of the file
            log_file.seek(0, 2)

            while True:
                line = log_file.readline()
                if not line:
                    time.sleep(0.1)
                    continue
                line = line.rstrip("\n")
                click.echo(line)
    except KeyboardInterrupt:
        cli_utils.declare(
            f"Stop monitoring orchestrator '{orchestrator_name}'."
        )
