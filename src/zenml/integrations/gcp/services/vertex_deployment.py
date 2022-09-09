#  Copyright (c) ZenML GmbH 2022. All Rights Reserved.
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

from typing import Optional, Generator, Tuple
from zenml.services import BaseService, ServiceConfig, ServiceType, ServiceState
from pydantic import Field

from zenml.services.service_status import ServiceStatus


class VertexDeploymentConfig(ServiceConfig):
    """Vertex AI deployment service configuration."""

    pass


class VertexDeploymentService(BaseService):
    """A ZenML service that represents a Vertex AI Endpoint."""

    SERVICE_TYPE = ServiceType(
        name="vertex-deployment",
        type="model-serving",
        flavor="vertex",
        description="Vertex AI Endpoint inference service",
    )

    config: VertexDeploymentConfig = Field(
        default_factory=VertexDeploymentConfig
    )
    status: ServiceStatus = Field(default_factory=ServiceStatus)

    def check_status(self) -> Tuple[ServiceState, str]:
        pass

    def get_logs(
        self, follow: bool = False, tail: Optional[int] = None
    ) -> Generator[str, bool, None]:
        pass

    def provision(self) -> None:
        pass

    def deprovision(self, force: bool = False) -> None:
        pass
