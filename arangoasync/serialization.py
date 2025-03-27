__all__ = [
    "Serializer",
    "Deserializer",
    "JsonSerializer",
    "JsonDeserializer",
    "DefaultSerializer",
    "DefaultDeserializer",
]

import json
from abc import ABC, abstractmethod
from typing import Generic, Sequence, TypeVar

from arangoasync.exceptions import DeserializationError, SerializationError
from arangoasync.typings import Json, Jsons

T = TypeVar("T")
U = TypeVar("U")


class Serializer(ABC, Generic[T]):  # pragma: no cover
    """Abstract base class for serialization.

    Custom serialization classes should inherit from this class.
    Please be mindful of the performance implications.
    """

    @abstractmethod
    def dumps(self, data: T | Sequence[T | str]) -> str:
        """Serialize any generic data.

        Args:
            data: Data to serialize.

        Returns:
            str: Serialized data.

        Raises:
            SerializationError: If the data cannot be serialized.
        """
        raise NotImplementedError


class Deserializer(ABC, Generic[T, U]):  # pragma: no cover
    """Abstract base class for deserialization.

    Custom deserialization classes should inherit from this class.
    Please be mindful of the performance implications.
    """

    @abstractmethod
    def loads(self, data: bytes) -> T:
        """Deserialize response data.

        Will be called on generic server data (such as server status) and
        single documents.

        Args:
            data (bytes): Data to deserialize.

        Returns:
            Deserialized data.

        Raises:
            DeserializationError: If the data cannot be deserialized.
        """
        raise NotImplementedError

    @abstractmethod
    def loads_many(self, data: bytes) -> U:
        """Deserialize response data.

        Will only be called when deserializing a list of documents.

        Args:
            data (bytes): Data to deserialize.

        Returns:
            Deserialized data.

        Raises:
            DeserializationError: If the data cannot be deserialized.
        """
        raise NotImplementedError


class JsonSerializer(Serializer[Json]):
    """JSON serializer."""

    def dumps(self, data: Json | Sequence[str | Json]) -> str:
        try:
            return json.dumps(data, separators=(",", ":"))
        except Exception as e:
            raise SerializationError("Failed to serialize data to JSON.") from e


class JsonDeserializer(Deserializer[Json, Jsons]):
    """JSON deserializer."""

    def loads(self, data: bytes) -> Json:
        try:
            return json.loads(data)  # type: ignore[no-any-return]
        except Exception as e:
            raise DeserializationError("Failed to deserialize data from JSON.") from e

    def loads_many(self, data: bytes) -> Jsons:
        return self.loads(data)  # type: ignore[return-value]


DefaultSerializer = JsonSerializer
DefaultDeserializer = JsonDeserializer
