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
"""Service implementation for the ZenML docker server deployment."""

import ipaddress
import os
import shutil
from typing import Any, Dict, List, Optional, Tuple, Union

from zenml.config.global_config import GlobalConfiguration  # type: ignore[import]

from zenml.constants import (
    DEFAULT_LOCAL_SERVICE_IP_ADDRESS,
    ENV_ZENML_CONFIG_PATH,
    ZEN_SERVER_ENTRYPOINT,
)
from zenml.enums import StoreType
from zenml.logger import get_logger
from zenml.services import (
    HTTPEndpointHealthMonitor,
    HTTPEndpointHealthMonitorConfig,
    BaseService,
    ServiceConfig,
    BaseServiceEndpoint,
    ServiceEndpointConfig,
    ServiceEndpointProtocol,
    ServiceType,
)
from zenml.utils.io_utils import get_global_config_directory
from zenml.zen_server.deploy.base_deployer import BaseServerDeploymentConfig

logger = get_logger(__name__)

ZEN_SERVER_HEALTHCHECK_URL_PATH = "health"

DOCKER_ZENML_SERVER_CONFIG_PATH = os.path.join(
    get_global_config_directory(),
    "zen_server",
    "docker",
)
DOCKER_ZENML_SERVER_CONFIG_FILENAME = os.path.join(
    DOCKER_ZENML_SERVER_CONFIG_PATH, "service.json"
)
DOCKER_ZENML_SERVER_GLOBAL_CONFIG_PATH = os.path.join(
    DOCKER_ZENML_SERVER_CONFIG_PATH, ".zenconfig"
)
DOCKER_ZENML_SERVER_DEFAULT_IMAGE = "zenml/zenml-server"


class DockerServerDeploymentConfig(BaseServerDeploymentConfig):
    """Docker server deployment configuration.

    Attributes:
        port: The TCP port number where the server is accepting connections.
        image: The Docker image to use for the server.
    """

    port: int = 8237
    image: str = DOCKER_ZENML_SERVER_DEFAULT_IMAGE


class DockerZenServerServiceConfig(ServiceConfig):
    """Docker ZenMl server service configuration.

    Attributes:
        server: docker ZenML server deployment configuration.
    """

    server: DockerServerDeploymentConfig


class DockerZenServer(BaseService):
    """Service that can be used to start a docker ZenServer.

    Attributes:
        config: service configuration
        endpoint: service endpoint
    """

    SERVICE_TYPE = ServiceType(
        name="docker_zenml_server",
        type="zen_server",
        flavor="docker",
        description="Docker ZenML server deployment",
    )

    config: DockerZenServerServiceConfig
    endpoint: BaseServiceEndpoint

    def __init__(
        self,
        server_config: Optional[DockerServerDeploymentConfig] = None,
        **attrs: Any,
    ) -> None:
        """Initialize the ZenServer.

        Args:
            server_config: server deployment configuration.
            attrs: Pydantic initialization arguments.
        """
        if server_config:
            # initialization from a server deployment configuration
            config, endpoint_cfg, monitor_cfg = self._get_configuration(
                server_config
            )
            attrs["config"] = config
            endpoint = BaseServiceEndpoint(
                config=endpoint_cfg,
                monitor=HTTPEndpointHealthMonitor(
                    config=monitor_cfg,
                ),
            )
            attrs["endpoint"] = endpoint

        super().__init__(**attrs)

    @classmethod
    def _get_configuration(
        cls,
        config: DockerServerDeploymentConfig,
    ) -> Tuple[
        DockerZenServerServiceConfig,
        ServiceEndpointConfig,
        HTTPEndpointHealthMonitorConfig,
    ]:
        """Construct the service configuration from a server deployment configuration.

        Args:
            config: server deployment configuration.

        Returns:
            The service, service endpoint and endpoint monitor configuration.
        """
        return (
            DockerZenServerServiceConfig(
                server=config,
                root_runtime_path=DOCKER_ZENML_SERVER_CONFIG_PATH,
                singleton=True,
            ),
            ServiceEndpointConfig(
            ),
            HTTPEndpointHealthMonitorConfig(
                healthcheck_uri_path=ZEN_SERVER_HEALTHCHECK_URL_PATH,
                use_head_request=True,
            ),
        )

    def _copy_global_configuration(self) -> str:
        """Copy the global configuration to the docker ZenML server location.

        The docker ZenML server global configuration is a copy of the docker
        global configuration with the store configuration set to point to the
        docker store.

        Returns:
            The path to the ZenML server global configuration.
        """
        gc = GlobalConfiguration()

        # this creates a copy of the global configuration with the store
        # set to the docker store and saves it to the server configuration path
        gc.copy_configuration(
            config_path=DOCKER_ZENML_SERVER_GLOBAL_CONFIG_PATH,
            store_config=gc.get_default_store(),
        )

    @classmethod
    def get_service(cls) -> Optional["DockerZenServer"]:
        """Load and return the docker ZenML server service, if present.

        Returns:
            The docker ZenML server service or None, if the docker server
            deployment is not found.
        """
        from zenml.services import ServiceRegistry

        try:
            with open(DOCKER_ZENML_SERVER_CONFIG_FILENAME, "r") as f:
                return ServiceRegistry().load_service_from_json(f.read())
        except FileNotFoundError:
            return None

    def _get_daemon_cmd(self) -> Tuple[List[str], Dict[str, str]]:
        """Get the command to start the daemon.

        Overrides the base class implementation to add the environment variable
        that forces the ZenML server to use the copied global config.

        Returns:
            The command to start the daemon and the environment variables to
            set for the command.
        """
        cmd, env = super()._get_daemon_cmd()
        env[ENV_ZENML_CONFIG_PATH] = DOCKER_ZENML_SERVER_GLOBAL_CONFIG_PATH
        return cmd, env

    def provision(self) -> None:
        """Provision the service."""
        self._copy_global_configuration()
        super().provision()

    def deprovision(self, force: bool = False) -> None:
        """Deprovision the service.

        Args:
            force: if True, the service daemon will be forcefully stopped
        """
        super().deprovision(force=force)
        shutil.rmtree(DOCKER_ZENML_SERVER_CONFIG_PATH)

    def run(self) -> None:
        """Run the ZenServer.

        Raises:
            ValueError: if started with a global configuration that connects to
            another ZenML server.
        """
        import uvicorn

        gc = GlobalConfiguration()
        if gc.store and gc.store.type == StoreType.REST:
            raise ValueError(
                "The ZenML server cannot be started with REST store type."
            )
        logger.info(
            "Starting ZenServer as blocking "
            "process... press CTRL+C once to stop it."
        )

        self.endpoint.prepare_for_start()

        try:
            uvicorn.run(
                ZEN_SERVER_ENTRYPOINT,
                host=str(self.config.server.address),
                port=self.config.server.port,
                log_level="info",
            )
        except KeyboardInterrupt:
            logger.info("ZenServer stopped. Resuming normal execution.")

    @property
    def zen_server_url(self) -> Optional[str]:
        """Get the URI where the service responsible for the ZenServer is running.

        Returns:
            The URI where the service can be contacted for requests,
                or None, if the service isn't running.
        """
        if not self.is_running:
            return None
        return self.endpoint.status.uri

    def update(self, config: DockerServerDeploymentConfig) -> None:
        """Update the ZenServer configuration.

        Args:
            config: new server configuration.
        """
        (
            self.config,
            self.endpoint.config,
            self.endpoint.monitor.config,
        ) = self._get_configuration(config)
