#  Copyright (c) ZenML GmbH 2023. All Rights Reserved.
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
"""Implementation of ZenML's pydantic materializer."""

import os
from typing import TYPE_CHECKING, Any, Dict, Type

from pydantic import BaseModel

from zenml.enums import ArtifactType
from zenml.materializers.base_materializer import BaseMaterializer
from zenml.utils import yaml_utils

if TYPE_CHECKING:
    from zenml.metadata.metadata_types import MetadataType

DEFAULT_FILENAME = "data.json"


class PydanticMaterializer(BaseMaterializer):
    """Handle Pydantic BaseModel objects."""

    ASSOCIATED_ARTIFACT_TYPE = ArtifactType.DATA
    ASSOCIATED_TYPES = (BaseModel,)

    def load(self, data_type: Type[BaseModel]) -> Any:
        """Reads BaseModel from JSON.

        Args:
            data_type: The type of the data to read.

        Returns:
            The data read.
        """
        contents = super().load(data_type)
        data_path = os.path.join(self.uri, DEFAULT_FILENAME)
        contents = yaml_utils.read_json(data_path)
        return data_type.parse_raw(contents)

    def save(self, data: BaseModel) -> None:
        """Serialize a BaseModel to JSON.

        Args:
            data: The data to store.
        """
        super().save(data)
        data_path = os.path.join(self.uri, DEFAULT_FILENAME)
        yaml_utils.write_json(data_path, data.json())

    def extract_metadata(self, data: BaseModel) -> Dict[str, "MetadataType"]:
        """Extract metadata from the given BaseModel object.

        Args:
            data: The BaseModel object to extract metadata from.

        Returns:
            The extracted metadata as a dictionary.
        """
        base_metadata = super().extract_metadata(data)
        container_metadata = {
            "schema": data.schema(),
        }
        return {**base_metadata, **container_metadata}
