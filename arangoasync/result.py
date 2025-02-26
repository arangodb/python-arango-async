__all__ = ["Result"]

from typing import TypeVar, Union

from arangoasync.job import AsyncJob

# The Result definition has to be in a separate module because of circular imports.
T = TypeVar("T")
Result = Union[T, AsyncJob[T], None]
