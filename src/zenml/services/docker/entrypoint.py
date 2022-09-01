#  Copyright (c) ZenML GmbH 2022. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Implementation of an entrypoint for a dockerized ZenML service.

This executable file is utilized as an entrypoint for all ZenML services
that are implemented as docker containers.
"""

import click

from zenml.integrations.registry import integration_registry
from zenml.logger import get_logger
from zenml.services import ServiceRegistry
from zenml.services.docker.docker_service import DockerService

logger = get_logger(__name__)

@click.command()
@click.option("--config-file", required=True, type=click.Path(exists=True))
def run(
    config_file: str,
) -> None:
    """Runs a ZenML service as a dockerized process.

    Args:
        config_file: path to the configuration file for the service.
    """
    logger.info("Loading service configuration from %s", config_file)
    with open(config_file, "r") as f:
        config = f.read()

    integration_registry.activate_integrations()

    logger.debug("Running service with configuration:\n %s", config)
    service = ServiceRegistry().load_service_from_json(config)
    if not isinstance(service, DockerService):
        raise TypeError(
            f"Expected service type DockerService but got "
            f"{type(service)} instead"
        )
    service.run()


if __name__ == "__main__":
    run()
