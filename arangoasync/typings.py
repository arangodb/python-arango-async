__all__ = [
    "RequestHeaders",
    "ResponseHeaders",
    "Params",
    "Result",
]

from typing import MutableMapping, TypeVar, Union

from multidict import CIMultiDictProxy, MultiDict

from arangoasync.job import AsyncJob

RequestHeaders = MutableMapping[str, str] | MultiDict[str]
RequestHeaders.__doc__ = """Type definition for request HTTP headers"""

ResponseHeaders = MutableMapping[str, str] | MultiDict[str] | CIMultiDictProxy[str]
ResponseHeaders.__doc__ = """Type definition for response HTTP headers"""

Params = MutableMapping[str, bool | int | str]
Params.__doc__ = """Type definition for URL (query) parameters"""

T = TypeVar("T")
Result = Union[T, AsyncJob[T]]
