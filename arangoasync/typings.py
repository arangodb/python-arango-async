__all__ = [
    "RequestHeaders",
    "ResponseHeaders",
    "Params",
]

from typing import MutableMapping

from multidict import CIMultiDictProxy, MultiDict

RequestHeaders = MutableMapping[str, str] | MultiDict[str]
RequestHeaders.__doc__ = """Type definition for request HTTP headers"""

ResponseHeaders = MutableMapping[str, str] | MultiDict[str] | CIMultiDictProxy[str]
ResponseHeaders.__doc__ = """Type definition for response HTTP headers"""

Params = MutableMapping[str, bool | int | str]
Params.__doc__ = """Type definition for URL (query) parameters"""
