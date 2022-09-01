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
"""Implementation of the ZenML service registry."""

import json
from typing import TYPE_CHECKING, Any, Dict, Optional, Type, cast
from uuid import UUID

from zenml.logger import get_logger
from zenml.services.service_type import ServiceType
from zenml.utils.singleton import SingletonMetaClass

logger = get_logger(__name__)

if TYPE_CHECKING:
    from zenml.services.service import BaseService


class ServiceRegistry(metaclass=SingletonMetaClass):
    """Registry of service types and service instances.

    The service registry provides a central place to register service types
    and instantiate services.
    """

    def __init__(self) -> None:
        """Initialize the service registry."""
        self.service_types: Dict[ServiceType, Type["BaseService"]] = {}

    def register_service_type(self, cls: Type["BaseService"]) -> None:
        """Registers a new service type.

        Args:
            cls: a BaseService subclass.

        Raises:
            TypeError: if the service type is already registered.
        """
        service_type = cls.SERVICE_TYPE
        if service_type not in self.service_types:
            self.service_types[service_type] = cls
            logger.debug(
                f"Registered service class {cls} for "
                f"service type `{service_type}`"
            )
        else:
            raise TypeError(
                f"Found existing service type for {service_type}: "
                f"{self.service_types[service_type]}. Skipping registration "
                f"of {cls}."
            )

    def get_service_type(
        self, service_type: ServiceType
    ) -> Optional[Type["BaseService"]]:
        """Get the service class registered for a service type.

        Args:
            service_type: service type.

        Returns:
            `BaseService` subclass that was registered for the service type or
            None, if no service class was registered for the service type.
        """
        return self.service_types.get(service_type)

    def get_service_types(
        self,
    ) -> Dict[ServiceType, Type["BaseService"]]:
        """Get all registered service types.

        Returns:
            Dictionary of service types indexed by their service type.
        """
        return self.service_types.copy()

    def service_type_is_registered(self, service_type: ServiceType) -> bool:
        """Check if a service type is registered.

        Args:
            service_type: service type.

        Returns:
            True, if a service type is registered for the service type, False
            otherwise.
        """
        return service_type in self.service_types

    def load_service_from_dict(
        self, service_dict: Dict[str, Any]
    ) -> "BaseService":
        """Load a service instance from its dict representation.

        Creates and returns a service instantiated from the dict
        representation of the service configuration and last known status
        information.

        Args:
            service_dict: dict representation of the service configuration and
                last known status

        Returns:
            A new or existing ZenML service instance.

        Raises:
            TypeError: if the service type is not registered.
            ValueError: if the service type is not valid.
        """
        service_type = service_dict.get("service_type")
        if not service_type:
            raise ValueError(
                "Service type not present in the service dictionary"
            )
        service_type = ServiceType.parse_obj(service_type)
        service_class = self.get_service_type(service_type)
        if not service_class:
            raise TypeError(
                f"Cannot load service with unregistered service "
                f"type: {service_type}"
            )
        service = cast("BaseService", service_class.from_dict(service_dict))
        return service

    def load_service_from_json(self, json_str: str) -> "BaseService":
        """Load a service instance from its JSON representation.

        Creates and returns a service instantiated from the JSON serialized
        service configuration and last known status information.

        Args:
            json_str: JSON string representation of the service configuration
                and last known status

        Returns:
            A ZenML service instance.
        """
        service_dict = json.loads(json_str)
        return self.load_service_from_dict(service_dict)
