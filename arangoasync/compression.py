__all__ = [
    "AcceptEncoding",
    "ContentEncoding",
    "CompressionManager",
    "DefaultCompressionManager",
]

import zlib
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Optional


class AcceptEncoding(Enum):
    """Valid accepted encodings for the Accept-Encoding header."""

    DEFLATE = auto()
    GZIP = auto()
    IDENTITY = auto()


class ContentEncoding(Enum):
    """Valid content encodings for the Content-Encoding header."""

    DEFLATE = auto()
    GZIP = auto()


class CompressionManager(ABC):  # pragma: no cover
    """Abstract base class for handling request/response compression."""

    @abstractmethod
    def needs_compression(self, data: str | bytes) -> bool:
        """Determine if the data needs to be compressed

        Args:
            data (str | bytes): Data to check

        Returns:
            bool: True if the data needs to be compressed
        """
        raise NotImplementedError

    @abstractmethod
    def compress(self, data: str | bytes) -> bytes:
        """Compress the data

        Args:
            data (str | bytes): Data to compress

        Returns:
            bytes: Compressed data
        """
        raise NotImplementedError

    @abstractmethod
    def content_encoding(self) -> str:
        """Return the content encoding.

        This is the value of the Content-Encoding header in the HTTP request.
        Must match the encoding used in the compress method.

        Returns:
            str: Content encoding
        """
        raise NotImplementedError

    @abstractmethod
    def accept_encoding(self) -> str | None:
        """Return the accept encoding.

        This is the value of the Accept-Encoding header in the HTTP request.
        Currently, only deflate and "gzip" are supported.

        Returns:
            str: Accept encoding
        """
        raise NotImplementedError


class DefaultCompressionManager(CompressionManager):
    """Compress requests using the deflate algorithm.

    Args:
        threshold (int): Will compress requests to the server if
        the size of the request body (in bytes) is at least the value of this option.
        Setting it to -1 will disable request compression (default).
        level (int): Compression level. Defaults to 6.
        accept (str | None): Accepted encoding. By default, there is
        no compression of responses.
    """

    def __init__(
        self,
        threshold: int = -1,
        level: int = 6,
        accept: Optional[AcceptEncoding] = None,
    ) -> None:
        self._threshold = threshold
        self._level = level
        self._content_encoding = ContentEncoding.DEFLATE.name.lower()
        self._accept_encoding = accept.name.lower() if accept else None

    def needs_compression(self, data: str | bytes) -> bool:
        return self._threshold != -1 and len(data) >= self._threshold

    def compress(self, data: str | bytes) -> bytes:
        if data is not None:
            if isinstance(data, bytes):
                return zlib.compress(data, self._level)
            return zlib.compress(data.encode("utf-8"), self._level)
        return b""

    def content_encoding(self) -> str:
        return self._content_encoding

    def accept_encoding(self) -> str | None:
        return self._accept_encoding