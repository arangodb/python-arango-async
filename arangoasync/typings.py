__all__ = [
    "Headers",
    "Params",
]

from typing import MutableMapping

from multidict import MultiDict

Headers = MutableMapping[str, str] | MultiDict[str]
Headers.__doc__ = """Type definition for HTTP headers"""

Params = MutableMapping[str, bool | int | str]
Params.__doc__ = """Type definition for URL (query) parameters"""
