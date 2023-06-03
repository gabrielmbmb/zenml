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
"""Argilla annotator flavor."""

from typing import TYPE_CHECKING, Optional, Type

from zenml.annotators.base_annotator import (
    BaseAnnotatorConfig,
    BaseAnnotatorFlavor,
)
from zenml.integrations.argilla import ARGILLA_ANNOTATOR_FLAVOR

if TYPE_CHECKING:
    from zenml.integrations.argilla.annotators import ArgillaAnnotator


class ArgillaAnnotatorConfig(BaseAnnotatorConfig):
    """Config for the Argilla annotator."""

    pass


class ArgillaAnnotatorFlavor(BaseAnnotatorFlavor):
    """Argilla annotator flavor."""

    @property
    def name(self) -> str:
        """Name of the flavor.

        Returns:
            The name of the flavor.
        """
        return ARGILLA_ANNOTATOR_FLAVOR

    @property
    def docs_url(self) -> Optional[str]:
        """A URL to the documentation for this flavor.

        Returns:
            URL to the documentation for this flavor.
        """
        return self.generate_default_docs_url()

    @property
    def sdk_docs_url(self) -> Optional[str]:
        """A URL to the SDK documentation for this flavor.

        Returns:
            URL to the SDK documentation for this flavor.
        """
        return self.generate_default_sdk_docs_url()

    @property
    def logo_url(self) -> Optional[str]:
        """A URL to a logo for this flavor.

        Returns:
            URL to a logo for this flavor.
        """
        return "https://public-flavor-logos.s3.eu-central-1.amazonaws.com/annotator/argilla.png"

    @property
    def config_class(self) -> Type[ArgillaAnnotatorConfig]:
        """The config class for the Argilla annotator.

        Returns:
            The config class for the Argilla annotator.
        """
        return ArgillaAnnotatorConfig

    @property
    def implementation_class(self) -> Type["ArgillaAnnotator"]:
        """The implementation class for the Argilla annotator.

        Returns:
            The implementation class for the Argilla annotator.
        """
        from zenml.integrations.argilla.annotators import ArgillaAnnotator

        return ArgillaAnnotator
