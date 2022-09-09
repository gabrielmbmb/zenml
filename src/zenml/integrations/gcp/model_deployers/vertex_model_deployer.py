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

from typing import TYPE_CHECKING, ClassVar, List, Optional

from zenml.integrations.gcp import GCP_VERTEX_MODEL_DEPLOYER_FLAVOR
from zenml.logger import get_logger
from zenml.model_deployers.base_model_deployer import BaseModelDeployer

if TYPE_CHECKING:
    from uuid import UUID

    from zenml.services import BaseService, ServiceConfig

logger = get_logger(__name__)

DEFAULT_VERTEX_DEPLOYMENT_START_STOP_TIMEOUT = 300


class VertexModelDeployer(BaseModelDeployer):
    """Vertex AI model deployer stack component implementation."""

    # Class variables
    FLAVOR: ClassVar[str] = GCP_VERTEX_MODEL_DEPLOYER_FLAVOR

    def deploy_model(
        self, config: ServiceConfig, replace: bool = False, timeout: int = ...
    ) -> BaseService:
        return super().deploy_model(config, replace, timeout)

    def find_model_server(
        self,
        running: bool = False,
        service_uuid: Optional[UUID] = None,
        pipeline_name: Optional[str] = None,
        pipeline_run_id: Optional[str] = None,
        pipeline_step_name: Optional[str] = None,
        model_name: Optional[str] = None,
        model_uri: Optional[str] = None,
        model_type: Optional[str] = None,
    ) -> List[BaseService]:
        pass

    def stop_model_server(
        self,
        uuid: UUID,
        timeout: int = DEFAULT_VERTEX_DEPLOYMENT_START_STOP_TIMEOUT,
        force: bool = False,
    ) -> None:
        pass

    def start_model_server(
        self,
        uuid: UUID,
        timeout: int = DEFAULT_VERTEX_DEPLOYMENT_START_STOP_TIMEOUT,
    ) -> None:
        pass

    def delete_model_server(
        self,
        uuid: UUID,
        timeout: int = DEFAULT_VERTEX_DEPLOYMENT_START_STOP_TIMEOUT,
        force: bool = False,
    ) -> None:
        pass
