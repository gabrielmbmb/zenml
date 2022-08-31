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
"""Zen Server docker deployer implementation."""

from typing import ClassVar, List, Optional, cast
from zenml.config.global_config import GlobalConfiguration
from zenml.enums import StoreType

from zenml.logger import get_logger
from zenml.zen_server.deploy.base_deployer import (
    BaseServerDeployer,
    BaseServerDeployment,
    BaseServerDeploymentConfig,
    BaseServerDeploymentStatus,
)
from zenml.zen_server.deploy.docker.docker_zen_server import (
    DockerZenServer,
    DockerServerDeploymentConfig,
)
from zenml.zen_stores.base_zen_store import DEFAULT_USERNAME
from zenml.zen_stores.rest_zen_store import RestZenStoreConfiguration

logger = get_logger(__name__)

DOCKER_PROVIDER_NAME = "docker"

DOCKER_SERVER_SINGLETON_NAME = "docker"

DOCKER_DEFAULT_TIMEOUT = 60


class DockerServerDeploymentStatus(BaseServerDeploymentStatus):
    """Docker server deployment status.

    Attributes:
    """

    url: str


class DockerServerDeployment(BaseServerDeployment):
    """Docker server deployment.

    Attributes:
        config: The docker server deployment configuration.
        status: The docker server deployment status.
    """

    config: DockerServerDeploymentConfig
    status: DockerServerDeploymentStatus


class DockerServerDeployer(BaseServerDeployer):
    """Docker ZenML server deployer."""

    PROVIDER: ClassVar[str] = DOCKER_PROVIDER_NAME

    def up(
        self,
        config: BaseServerDeploymentConfig,
        connect: bool = True,
        timeout: Optional[int] = None,
    ) -> None:
        """Deploy the docker ZenML server instance.

        This starts a daemon process that runs the uvicorn server directly on
        the docker host configured to use the docker SQL store.

        Args:
            config: The server deployment configuration.
            connect: Set to connect to the server after deployment.
            timeout: The timeout in seconds to wait until the deployment is
                successful. If not supplied, a default timeout value of 30
                seconds is used.
        """
        if not isinstance(config, DockerServerDeploymentConfig):
            raise TypeError(
                "Invalid server deployment configuration type. It should be a "
                "DockerServerDeploymentConfig."
            )
        docker_config = cast(DockerServerDeploymentConfig, config)

        service = DockerZenServer.get_service()
        if service is not None:
            if service.config.server == docker_config:
                logger.info(
                    "The docker ZenML server is already running with the same "
                    "configuration."
                )
            else:
                logger.info(
                    "The docker ZenML server is already running with a "
                    "different configuration."
                )
                logger.info("Updating the docker ZenML server.")
                service.stop(timeout=timeout or DOCKER_DEFAULT_TIMEOUT)
                service.update(docker_config)
        else:
            logger.info("Starting the docker ZenML server.")
            service = DockerZenServer(docker_config)

        if not service.is_running:
            service.start(timeout=timeout or DOCKER_DEFAULT_TIMEOUT)

        if connect:
            self.connect(
                DOCKER_SERVER_SINGLETON_NAME,
                username=docker_config.username,
                password=docker_config.password,
            )

    def down(self, server: str, timeout: Optional[int] = None) -> None:
        """Tear down the docker ZenML server instance.

        Args:
            server: The server deployment name or identifier.

        Raises:
            KeyError: If the docker server deployment is not found.
        """

        service = DockerZenServer.get_service()
        if service is None:
            raise KeyError("The docker ZenML server is not deployed.")

        self.disconnect(server)

        logger.info("Shutting down the docker ZenML server.")
        service.stop(timeout=timeout or DOCKER_DEFAULT_TIMEOUT)

    def status(self, server: str) -> BaseServerDeploymentStatus:
        """Get the status of the docker ZenML server instance.

        Args:
            server: The server deployment name or identifier.

        Returns:
            The server deployment status.
        """
        docker_server = self.get(DOCKER_SERVER_SINGLETON_NAME)
        return docker_server.status

    def connect(self, server: str, username: str, password: str) -> None:
        """Connect to the docker ZenML server instance.

        Args:
            server: The server deployment name, identifier or URL.
            username: The username to use to connect to the server.
            password: The password to use to connect to the server.
        """

        gc = GlobalConfiguration()

        if server != DOCKER_SERVER_SINGLETON_NAME:
            raise KeyError(
                f"The {server} docker ZenML server could not be found."
            )

        service = DockerZenServer.get_service()
        if service is None:
            raise KeyError("The docker ZenML server could not be found.")

        url = service.zen_server_url
        if not url:
            raise RuntimeError("The docker ZenML server is not accessible.")

        store_config = RestZenStoreConfiguration(
            url=url, username=DEFAULT_USERNAME, password=""
        )

        if gc.store == store_config:
            logger.info("ZenML is already connected to the docker ZenML server.")
            return

        gc.set_store(store_config)

    def disconnect(self, server: str) -> None:
        """Disconnect from the docker ZenML server instance.

        Args:
            server: The server deployment name, identifier or URL.
        """

        gc = GlobalConfiguration()

        if not gc.store or gc.store.type != StoreType.REST:
            logger.info("ZenML is not currently connected to a ZenML server.")
            return

        if server != DOCKER_SERVER_SINGLETON_NAME:
            raise KeyError(
                f"The {server} docker ZenML server could not be found."
            )

        service = DockerZenServer.get_service()
        if service is None:
            raise KeyError("The docker ZenML server could not be found.")

        url = service.zen_server_url
        # TODO: we must be able to disconnect from a server even when it's
        # not accessible.
        if not url:
            raise RuntimeError("The docker ZenML server is not accessible.")

        if gc.store.url != url:
            logger.info(
                "ZenML is not currently connected to the docker ZenML server."
            )
            return

        gc.set_default_store()

    def get(self, server: str) -> BaseServerDeployment:
        """Get the docker server deployment.

        Args:
            server: The server deployment name, identifier or URL.

        Returns:
            The requested server deployment or None, if no server deployment
            could be found corresponding to the given name, identifier or URL.

        Raises:
            KeyError: If the server deployment is not found.
        """

        from zenml.services import ServiceState

        if server != DOCKER_SERVER_SINGLETON_NAME:
            raise KeyError(
                f"The {server} docker ZenML server could not be found."
            )

        service = DockerZenServer.get_service()
        if service is None:
            raise KeyError("The docker ZenML server could not be found.")

        service_status = service.check_status()
        gc = GlobalConfiguration()
        url = service.zen_server_url or ""
        connected = url and gc.store and gc.store.url == url

        return DockerServerDeployment(
            config=service.config,
            status=DockerServerDeploymentStatus(
                url=url,
                deployed=True,
                running=service_status[0] == ServiceState.ACTIVE,
                connected=connected,
            ),
        )

    def list(self) -> List[BaseServerDeployment]:
        """List all server deployments.

        Returns:
            The list of server deployments.
        """
        try:
            docker_server = self.get(DOCKER_SERVER_SINGLETON_NAME)
            return [docker_server]
        except KeyError:
            return []
