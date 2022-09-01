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
"""Implementation of a docker ZenML service."""

from cProfile import label
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time
from abc import ABC, abstractmethod
from typing import Dict, Generator, List, Optional, Tuple, Union
from uuid import UUID

import psutil
from psutil import NoSuchProcess
from pydantic import BaseModel, Field, validator

from zenml.logger import get_logger
from zenml.services.docker.docker_service_endpoint import (
    DockerServiceEndpoint,
)
from zenml.services.service import BaseService, ServiceConfig
from zenml.services.service_registry import ServiceRegistry
from zenml.services.service_status import ServiceState, ServiceStatus
from zenml.utils.io_utils import create_dir_recursive_if_not_exists
import docker

logger = get_logger(__name__)


SERVICE_CONFIG_FILE_NAME = "service.json"
SERVICE_LOG_FILE_NAME = "service.log"
ENV_ZENML_SERVICE_CONTAINER = "ZENML_SERVICE_CONTAINER"
SERVICE_CONTAINER_PATH = "/service"
DOCKER_ZENML_SERVER_DEFAULT_IMAGE = "zenml/zenml"


class RunnableServiceMixin(ABC):
    """Mixin class for all runnable services.

    All services that need to provide a service implementation should
    inherit from this class and implement the `run` method.
    """

    @abstractmethod
    def run(self) -> None:
        """Run the logic associated with this service.

        Subclasses must implement this method to provide the service
        functionality. This method will be executed in the context of the
        running service, not in the context of the process that calls the
        service `start` method.
        """
        raise NotImplementedError


class DockerServiceConfig(ServiceConfig):
    """Docker service configuration.

    Attributes:
        silent: set to True to suppress the output of the dockerized
            service (i.e. redirect stdout and stderr to /dev/null). If False,
            the service output will be redirected to a logfile.
        image: The docker image to use for the service. If not set, the default
            image configured in the DockerServiceEngine is used.
        timeout: The timeout in seconds to wait when starting, stopping or
            determining the service runtime status. If not set, the default
            timeout configured in the ServiceEngine is used.
    """

    silent: bool = False
    image: Optional[str] = None
    timeout: Optional[int] = None


class DockerServiceStatus(ServiceStatus):
    """Docker service status.

    Attributes:
        container_id: The container id of the service.
    """

    container_id: Optional[str] = None


class RunnableServiceEngine(BaseModel):
    """A service engine that manages ZenML services as locally executable processes.

    Attributes:
        root_path: the root path where all service files are persisted.
        singleton: set to True to instruct this engine to manage a single
            service instance instead of multiple services. This also causes the
            service files to be stored directly in the `root_path` directory
            instead of creating a subdirectory for each service instance.
    """

    root_path: str
    singleton: bool = False

    @property
    def root_runtime_path(self) -> str:
        """Return the root runtime path where all service files are located.

        Returns:
            The root runtime path where all service files are located.
        """
        return self.root_path

    def initialize(self) -> None:
        """Initialize the service engine.

        This method is called before any service is started. It creates the
        root runtime path if it does not exist.
        """
        if not os.path.exists(self.root_runtime_path):
            create_dir_recursive_if_not_exists(self.root_runtime_path)

    def get_service_path(self, service_id: Optional[UUID]) -> str:
        """Return the path where the files of a particular service are located.

        Args:
            service_id: The ID of the service to get the path for. If the
                service engine is configured to manage a single service, this
                parameter is not required.

        Returns:
            The path where the files for the supplied service are located.

        Raises:
            ValueError: if the service engine is configured to manage multiple
                services and the service ID is not supplied.
        """
        runtime_path: str = self.root_runtime_path
        if not self.singleton:
            if service_id is None:
                raise ValueError(
                    "The service ID is required when the service engine is not "
                    "managing a single service"
                )
            runtime_path = os.path.join(
                runtime_path,
                str(service_id),
            )
        return runtime_path

    def get_config_file_path(self, service_id: Optional[UUID]) -> str:
        """Get the path to the configuration file for a service.

        Args:
            service_id: The ID of the service to get the path for. If the
                service engine is configured to manage a single service, this
                parameter is not required.

        Returns:
            The path to the service configuration file.
        """
        return os.path.join(
            self.get_service_path(service_id), SERVICE_CONFIG_FILE_NAME
        )

    def get_log_file_path(self, service_id: Optional[UUID]) -> str:
        """Get the path to the log file for a service.

        Args:
            service_id: The ID of the service to get the path for. If the
                service engine is configured to manage a single service, this
                parameter is not required.

        Returns:
            The path to the service log file.
        """
        return os.path.join(
            self.get_service_path(service_id), SERVICE_LOG_FILE_NAME
        )

    def _load_service(self, config_file_path: str) -> BaseService:
        """Load a service from a configuration file.

        Args:
            config_file_path: The path to the service configuration file.

        Returns:
            The service loaded from the configuration file.
        """
        with open(config_file_path, "r") as f:
            service_config = f.read()

        return ServiceRegistry().load_service_from_json(service_config)

    def get_services(self) -> Generator[BaseService, None, None]:
        """Get all services managed by the service engine.

        Returns:
            A generator over all services managed by the service engine.
        """
        if self.singleton:
            service_config_file = self.get_config_file_path()
            if os.path.exists(service_config_file):
                yield self._load_service(service_config_file)
        else:
            for service_id in os.listdir(self.root_runtime_path):
                service_config_file = os.path.join(
                    self.root_runtime_path, service_id, SERVICE_CONFIG_FILE_NAME
                )
                if os.path.exists(service_config_file):
                    yield self._load_service(service_config_file)


class DockerServiceEngine(RunnableServiceEngine):
    """A service engine that manages ZenML services as docker containers.

    This service engine hosts all the core logic related to managing ZenML
    services as docker containers:

    * creating and starting a docker container from a service configuration
    * discovering services configured in a previous session
    * updating the operational status of a service to reflect the runtime state
    of the docker container that is hosting the service, if it exists
    * stopping docker containers and removing allocated resources

    Attributes:
        default_image: the default docker image to use for the services. This
            can be overridden in the service configuration.
    """

    image: str = DOCKER_ZENML_SERVER_DEFAULT_IMAGE

    _docker_client: Optional[docker.DockerClient] = None

    @property
    def docker_client(self) -> docker.DockerClient:
        """Initialize and/or return the docker client.

        Returns:
            The docker client.
        """
        if self._docker_client is None:
            self._docker_client = docker.from_env()
        return self._docker_client

    @property
    def inside_container(self) -> bool:
        """Return True if the service engine is running inside a container.

        Returns:
            True if the service engine is running inside a container.
        """
        return os.environ.get("ENV_ZENML_SERVICE_CONTAINER") is not None

    @property
    def root_runtime_path(self) -> str:
        """Return the root runtime path where all service files are located.

        The root runtime path is different from the root path if the service
        engine is running inside a container.

        Returns:
            The root runtime path where all service files are located.
        """
        if self.inside_container:
            return SERVICE_CONTAINER_PATH
        return super().root_runtime_path

    def get_service_path(self, service_id: Optional[UUID]) -> str:
        """Return the path where the files of a particular service are located.

        Args:
            service_id: The ID of the service to get the path for. If the
                service engine is configured to manage a single service, this
                parameter is not required.

        Returns:
            The path where the files for the supplied service are located.

        Raises:
            ValueError: if the service engine is configured to manage multiple
                services and the service ID is not supplied.
        """
        # when running inside a container, it's as if the service is a
        # singleton: the service engine is managing a single service instance
        if self.inside_container:
            return self.root_runtime_path    
        return super().get_service_path(service_id)

    def get_container_id(self, service_id: UUID) -> str:
        """Get the ID of the docker container for a service.

        Args:
            service_id: The ID of the service to get the name for.

        Returns:
            The name of the docker container for the service.
        """
        return f"zenml-{str(service_id)}"

    def create_service(self, service: BaseService) -> None:
        """Create a service.

        Args:
            service: The service to create.
        """
        # to avoid circular imports, import here
        import zenml.services.docker.entrypoint as entrypoint

        if not isinstance(service.config, DockerServiceConfig):
            raise ValueError(
                "Service config must be an instance of DockerServiceConfig"
            )
        if not isinstance(service, RunnableServiceMixin):
            raise ValueError(
                "Service must extend RunnableServiceMixin and implement the "
                "run() method"
            )

        container: Optional[docker.containers.Container] = None
        try:
            container = self.docker_client.containers.get(
                self.get_container_name(service)
            )
        except docker.errors.NotFound:
            # container doesn't exist yet or was removed
            return

        if container:
            # the container exists, check if it is running
            if container.status == "running":
                logger.debug(
                    "Container for service '%s' is already running",
                    service,
                )
                return

            # the container is stopped or in an error state, remove it
            logger.debug(
                "Removing previous container for service '%s'",
                service,
            )

        service_config: DockerServiceConfig = service.config
        image = service_config.image
        if image is None:
            image = self.image
        service_path = self.get_service_path(service)

        create_dir_recursive_if_not_exists(service_path)
        with open(self.get_config_file_path(service), "w") as f:
            f.write(self.json(indent=4))

        command = [
            "python",
            "-m",
            entrypoint.__name__,
            "--config-file",
            os.path.join(SERVICE_CONTAINER_PATH, SERVICE_CONFIG_FILE_NAME),
        ]

        ports: Dict[int, Optional[int]] = {}
        # for endpoint in service.endpoints:
        #     ports[endpoint.port] = endpoint.port

        try:
            self.docker_client.containers.create(
                name=self.get_container_id(service),
                image=image,
                command=command,
                detach=True,
                volumes={
                    service_path: {
                        "bind": SERVICE_CONTAINER_PATH,
                        "mode": "rw",
                    },
                },
                environment={
                    ENV_ZENML_SERVICE_CONTAINER: "true",
                },
                remove=False,
                auto_remove=False,
                timeout=service_config.timeout,
                ports=ports,
                labels={
                    "zenml-service-uuid": service.uuid,
                },
                working_dir=SERVICE_CONTAINER_PATH,
            )
        except docker.errors.DockerException as e:
            raise RuntimeError(
                f"Failed to create Docker container for service {service.uuid}: {e}"
            ) from e

    def stop_service(self, service: BaseService) -> None:
        """Stop a service.

        Args:
            service: The service to stop.
        """
        if not isinstance(service, RunnableServiceMixin):
            raise ValueError(
                "Service must extend RunnableServiceMixin and implement the "
                "run() method"
            )

        container: Optional[docker.containers.Container] = None
        try:
            container = self.docker_client.containers.get(
                self.get_container_name(service)
            )
        except docker.errors.NotFound:
            # container doesn't exist yet or was removed
            return

        container.stop()

    def remove_service(self, service: BaseService) -> None:
        """Remove all resources and files associated with a service.

        Args:
            service: The service to remove.
        """
        if not isinstance(service, RunnableServiceMixin):
            raise ValueError(
                "Service must extend RunnableServiceMixin and implement the "
                "run() method"
            )

        self.stop_service(service)
        service_path = self.get_service_path(service.uuid)
        if os.path.exists(service_path):
            shutil.rmtree(service_path)

    def check_service_status(
        self, service: BaseService
    ) -> Tuple[ServiceState, str]:
        """Check the the current operational state of a dockerized service.

        Args:
            service: The service to get the status for.

        Returns:
            The operational state of the external service and a message
            providing additional information about that state (e.g. a
            description of the error if one is encountered while checking the
            service status).
        """
        if not isinstance(service, RunnableServiceMixin):
            raise ValueError(
                "Service must extend RunnableServiceMixin and implement the "
                "run() method"
            )

        container: Optional[docker.containers.Container] = None
        try:
            container = self.docker_client.containers.get(
                self.get_container_name(service)
            )
        except docker.errors.NotFound:
            # container doesn't exist yet or was removed
            pass

        if container is None:
            return ServiceState.INACTIVE, "Docker container is not present"
        elif container.status != "running":
            return (
                ServiceState.INACTIVE,
                f"Docker container is {container.status}",
            )
        else:
            return ServiceState.ACTIVE, "Docker container is running"


class DockerService(BaseService):
    """A service represented by a docker daemon process.

    This class extends the base service class with functionality concerning
    the life-cycle management and tracking of external services implemented as
    dockerized processes.

    To define a docker daemon service, subclass this class and implement the
    `run` method. Upon `start`, the service will spawn a daemon process that
    ends up calling the `run` method.

    Example:

    ```python

    from zenml.services import ServiceType, DockerService, DockerServiceConfig
    import time

    class SleepingDaemonConfig(DockerServiceConfig):

        wake_up_after: int

    class SleepingDaemon(DockerService):

        SERVICE_TYPE = ServiceType(
            name="sleeper",
            description="Sleeping daemon",
            type="daemon",
            flavor="sleeping",
        )
        config: SleepingDaemonConfig

        def run(self) -> None:
            time.sleep(self.config.wake_up_after)

    daemon = SleepingDaemon(config=SleepingDaemonConfig(wake_up_after=10))
    daemon.start()
    ```

    NOTE: the `SleepingDaemon` class and its parent module have to be
    discoverable as part of a ZenML `Integration`, otherwise the daemon will
    fail with the following error:

    ```
    TypeError: Cannot load service with unregistered service type:
    name='sleeper' type='daemon' flavor='sleeping' description='Sleeping daemon'
    ```


    Attributes:
        config: service configuration
        status: service status
        endpoint: optional service endpoint
    """

    config: DockerServiceConfig = Field(default_factory=DockerServiceConfig)
    status: DockerServiceStatus = Field(default_factory=DockerServiceStatus)
    # TODO [ENG-705]: allow multiple endpoints per service
    endpoint: Optional[DockerServiceEndpoint] = None

    def get_service_status_message(self) -> str:
        """Get a message about the current operational state of the service.

        Returns:
            A message providing information about the current operational
            state of the service.
        """
        msg = super().get_service_status_message()
        pid = self.status.pid
        if pid:
            msg += f"  Daemon PID: `{self.status.pid}`\n"
        if self.status.log_file:
            msg += (
                f"For more information on the service status, please see the "
                f"following log file: {self.status.log_file}\n"
            )
        return msg

    def check_status(self) -> Tuple[ServiceState, str]:
        """Check the the current operational state of the daemon process.

        Returns:
            The operational state of the daemon process and a message
            providing additional information about that state (e.g. a
            description of the error, if one is encountered).
        """
        if not self.status.pid:
            return ServiceState.INACTIVE, "service daemon is not running"

        # the daemon is running
        return ServiceState.ACTIVE, ""

    def _get_daemon_cmd(self) -> Tuple[List[str], Dict[str, str]]:
        """Get the command to run the service daemon.

        The default implementation provided by this class is the following:

          * this DockerService instance and its configuration
          are serialized as JSON and saved to a temporary file
          * the local_daemon_entrypoint.py script is launched as a subprocess
          and pointed to the serialized service file
          * the entrypoint script re-creates the DockerService instance
          from the serialized configuration, reconfigures itself as a daemon
          and detaches itself from the parent process, then calls the `run`
          method that must be implemented by the subclass

        Subclasses that need a different command to launch the service daemon
        should override this method.

        Returns:
            Command needed to launch the daemon process and the environment
            variables to set, in the formats accepted by subprocess.Popen.
        """
        # to avoid circular imports, import here
        import zenml.services.docker.entrypoint as entrypoint

        self.status.silent_daemon = self.config.silent_daemon
        # reuse the config file and logfile location from a previous run,
        # if available
        if not self.status.runtime_path or not os.path.exists(
            self.status.runtime_path
        ):
            if self.config.root_runtime_path:
                if self.config.singleton:
                    self.status.runtime_path = self.config.root_runtime_path
                else:
                    self.status.runtime_path = os.path.join(
                        self.config.root_runtime_path,
                        str(self.uuid),
                    )
                create_dir_recursive_if_not_exists(self.status.runtime_path)
            else:
                self.status.runtime_path = tempfile.mkdtemp(
                    prefix="zenml-service-"
                )

        assert self.status.config_file is not None
        assert self.status.pid_file is not None

        with open(self.status.config_file, "w") as f:
            f.write(self.json(indent=4))

        # delete the previous PID file, in case a previous daemon process
        # crashed and left a stale PID file
        if os.path.exists(self.status.pid_file):
            os.remove(self.status.pid_file)

        command = [
            sys.executable,
            "-m",
            entrypoint.__name__,
            "--config-file",
            self.status.config_file,
            "--pid-file",
            self.status.pid_file,
        ]
        if self.status.log_file:
            pathlib.Path(self.status.log_file).touch()
            command += ["--log-file", self.status.log_file]

        command_env = os.environ.copy()

        return command, command_env

    def _start_daemon(self) -> None:
        """Start the service daemon process associated with this service."""
        pid = self.status.pid
        if pid:
            # service daemon is already running
            logger.debug(
                "Daemon process for service '%s' is already running with PID %d",
                self,
                pid,
            )
            return

        logger.debug("Starting daemon for service '%s'...", self)

        if self.endpoint:
            self.endpoint.prepare_for_start()

        command, command_env = self._get_daemon_cmd()
        logger.debug(
            "Running command to start daemon for service '%s': %s",
            self,
            " ".join(command),
        )
        p = subprocess.Popen(command, env=command_env)
        p.wait()
        pid = self.status.pid
        if pid:
            logger.debug(
                "Daemon process for service '%s' started with PID: %d",
                self,
                pid,
            )
        else:
            logger.error(
                "Daemon process for service '%s' failed to start.",
                self,
            )

    def _stop_daemon(self, force: bool = False) -> None:
        """Stop the service daemon process associated with this service.

        Args:
            force: if True, the service daemon will be forcefully stopped
        """
        pid = self.status.pid
        if not pid:
            # service daemon is not running
            logger.debug(
                "Daemon process for service '%s' no longer running",
                self,
            )
            return

        logger.debug("Stopping daemon for service '%s' ...", self)
        try:
            p = psutil.Process(pid)
        except psutil.Error:
            logger.error(
                "Could not find process for for service '%s' ...", self
            )
            return
        if force:
            p.kill()
        else:
            p.terminate()

    def provision(self) -> None:
        """Provision the service."""
        self._start_daemon()

    def deprovision(self, force: bool = False) -> None:
        """Deprovision the service.

        Args:
            force: if True, the service daemon will be forcefully stopped
        """
        self._stop_daemon(force)

    def get_logs(
        self, follow: bool = False, tail: Optional[int] = None
    ) -> Generator[str, bool, None]:
        """Retrieve the service logs.

        Args:
            follow: if True, the logs will be streamed as they are written
            tail: only retrieve the last NUM lines of log output.

        Yields:
            A generator that can be accessed to get the service logs.
        """
        if not self.status.log_file or not os.path.exists(self.status.log_file):
            return

        with open(self.status.log_file, "r") as f:
            if tail:
                # TODO[ENG-864]: implement a more efficient tailing mechanism that
                #   doesn't read the entire file
                lines = f.readlines()[-tail:]
                for line in lines:
                    yield line.rstrip("\n")
                if not follow:
                    return
            line = ""
            while True:
                partial_line = f.readline()
                if partial_line:
                    line += partial_line
                    if line.endswith("\n"):
                        stop = yield line.rstrip("\n")
                        if stop:
                            break
                        line = ""
                elif follow:
                    time.sleep(1)
                else:
                    break

    @abstractmethod
    def run(self) -> None:
        """Run the service daemon process associated with this service.

        Subclasses must implement this method to provide the service daemon
        functionality. This method will be executed in the context of the
        running daemon, not in the context of the process that calls the
        `start` method.
        """
