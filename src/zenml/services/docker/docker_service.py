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

import os
import pathlib
import subprocess
import sys
import tempfile
import time
from abc import abstractmethod
from typing import Dict, Generator, List, Optional, Tuple, Union

import psutil
from psutil import NoSuchProcess
from pydantic import BaseModel, Field, validator

from zenml.logger import get_logger
from zenml.services.docker.docker_service_endpoint import (
    DockerServiceEndpoint,
)
from zenml.services.service import BaseService, ServiceConfig
from zenml.services.service_status import ServiceState, ServiceStatus
from zenml.utils.io_utils import create_dir_recursive_if_not_exists

logger = get_logger(__name__)


SERVICE_CONFIG_FILE_NAME = "service.json"
SERVICE_LOG_FILE_NAME = "service.log"
ENV_ZENML_SERVICE_CONTAINER = "ZENML_SERVICE_CONTAINER"
SERVICE_CONTAINER_PATH = "/service"
DOCKER_ZENML_SERVER_DEFAULT_IMAGE = "zenml/zenml"


class DockerServiceConfig(ServiceConfig):
    """Docker service configuration.

    Attributes:
        image: The docker image to use for the service. If not set, the default
            image configured in the DockerServiceEngine is used.
        timeout: The timeout in seconds to wait when starting, stopping or
            determining the service runtime status. If not set, the default
            timeout configured in the ServiceEngine is used.
    """
    image: Optional[str] = None
    timeout: Optional[int] = None


class DockerServiceStatus(ServiceStatus):
    """Docker service status.

    Attributes:
        container_id: The container id of the service.
    """

    container_id: Optional[str] = None


class DockerServiceEngine(BaseModel):
    """A service engine that manages ZenML services as docker containers.

    This service engine hosts all the core logic related to managing ZenML
    services as docker containers:

    * creating and starting a docker container from a service configuration
    * discovering services configured in a previous session
    * updating the operational status of a service to reflect the runtime state
    of the docker container that is hosting the service, if it exists
    * stopping docker containers and removing allocated resources

    Attributes:
        root_path: the root path where all service configuration files
            and logs are persisted. If not set, a temporary directory is used.
        singleton: set to True to instruct this engine to manage a single
            service instance instead of multiple services. This also causes the
            service configuration files to be stored directly in the
            `root_runtime_path` directory instead of creating a subdirectory for
            each service instance.
        default_image: the default docker image to use for the services. This
            can be overridden in the service configuration.
    """

    root_path: str
    singleton: bool = False
    image: str = DOCKER_ZENML_SERVER_DEFAULT_IMAGE

    @validator("root_path", pre=True)
    def validate_root_runtime_path(cls, value: Union[None, str]) -> str:
        """Validate the root path.

        If not set, a temporary directory is used..
        """
        if value is None:
            return tempfile.mkdtemp(prefix="zenml-docker-service-")
        return value

    @property
    def inside_container(self) -> bool:
        """Return True if the service engine is running inside a container.

        Returns:
            True if the service engine is running inside a container.
        """
        return os.environ.get("ENV_ZENML_SERVICE_CONTAINER") is not None

    @property
    def root_runtime_path(self) -> str:
        """Return the root runtime path where all service configuration and logs are located.

        The root runtime path is different from the root path if the service
        engine is running inside a container.

        Returns:
            The root runtime path where all service configuration and logs are
            located.
        """
        if self.inside_container:
            return SERVICE_CONTAINER_PATH
        return self.root_path

    def initialize(self) -> None:
        """Initialize the service engine.

        This method is called before any service is started. It creates the
        root runtime path if it does not exist.
        """
        if not os.path.exists(self.root_runtime_path):
            create_dir_recursive_if_not_exists(self.root_runtime_path)

    def get_service_path(self, service: BaseService) -> str:
        """Return the path where the configuration and logs of a particular service are located.

        Args:
            service: The service to get the path for.

        Returns:
            The path where the configuration and logs for the supplied service
            are located.
        """
        runtime_path: str = self.root_runtime_path
        if not self.singleton:
            runtime_path = os.path.join(
                runtime_path,
                str(service.uuid),
            )

    def get_config_file_path(self, service: BaseService) -> str:
        """Get the path to the configuration file for a service.

        Args:
            service: The service to get the path for.

        Returns:
            The path to the service configuration file.
        """
        return os.path.join(
            self.get_service_path(service), SERVICE_CONFIG_FILE_NAME
        )

    def get_log_file_path(self, service: BaseService) -> str:
        """Get the path to the log file for a service.

        Args:
            service: The service to get the path for.

        Returns:
            The path to the service log file.
        """
        return os.path.join(
            self.get_service_path(service), SERVICE_LOG_FILE_NAME
        )


class DockerService(BaseService):
    """A service represented by a docker daemon process.

    This class extends the base service class with functionality concerning
    the life-cycle management and tracking of external services implemented as
    docker daemon processes.

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
