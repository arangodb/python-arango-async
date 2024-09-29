__all__ = [
    "Serializer",
    "Deserializer",
    "JsonSerializer",
    "JsonDeserializer",
    "DefaultSerializer",
    "DefaultDeserializer",
]

from abc import ABC, abstractmethod
from json import dumps, loads
from typing import Any


class Serializer(ABC):  # pragma: no cover
    """Abstract base class for serialization.

    Custom serialization classes should inherit from this class.
    """

    @abstractmethod
    def to_str(self, data: Any) -> str:
        """Serialize any generic data.

        This method impacts all serialization operations within the client.
        Please be mindful of the performance implications.

        Args:
            data: Data to serialize.

        Returns:
            str: Serialized data.
        """
        raise NotImplementedError


class Deserializer(ABC):  # pragma: no cover
    """Abstract base class for deserialization.

    Custom deserialization classes should inherit from this class.
    """

    @abstractmethod
    def from_bytes(self, data: bytes) -> Any:
        """Deserialize generic response data that does not represent documents.

        This is to be used when the response is not a document, but some other
        information (for example, server status).

        Args:
            data (bytes): Data to deserialize.

        Returns:
            Deserialized data.

        Raises:
            json.JSONDecodeError: If the data cannot be deserialized.
        """
        raise NotImplementedError

    @abstractmethod
    def from_doc(self, data: bytes) -> Any:
        """Deserialize document data.

        This is to be used when the response represents (a) document(s).
        The implementation **must** support deserializing both a single documents
        and a list of documents.

        Args:
            data (bytes): Data to deserialize.

        Returns:
            Deserialized data.
        """
        raise NotImplementedError


class JsonSerializer(Serializer):
    """JSON serializer."""

    def to_str(self, data: Any) -> str:
        return dumps(data, separators=(",", ":"))


class JsonDeserializer(Deserializer):
    """JSON deserializer."""

    def from_bytes(self, data: bytes) -> Any:
        return loads(data)

    def from_doc(self, data: bytes) -> Any:
        return loads(data)


DefaultSerializer = JsonSerializer
DefaultDeserializer = JsonDeserializer
