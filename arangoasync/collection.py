__all__ = ["Collection", "StandardCollection"]


from typing import Generic, Optional, Tuple, TypeVar

from arangoasync.errno import HTTP_NOT_FOUND, HTTP_PRECONDITION_FAILED
from arangoasync.exceptions import (
    DocumentGetError,
    DocumentParseError,
    DocumentRevisionError,
)
from arangoasync.executor import ApiExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import Json, Result

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


class Collection(Generic[T, U, V]):
    """Base class for collection API wrappers.

    Args:
        executor (ApiExecutor): API executor.
        name (str): Collection name
        doc_serializer (Serializer): Document serializer.
        doc_deserializer (Deserializer): Document deserializer.
    """

    def __init__(
        self,
        executor: ApiExecutor,
        name: str,
        doc_serializer: Serializer[T],
        doc_deserializer: Deserializer[U, V],
    ) -> None:
        self._executor = executor
        self._name = name
        self._doc_serializer = doc_serializer
        self._doc_deserializer = doc_deserializer
        self._id_prefix = f"{self._name}/"

    def __repr__(self) -> str:
        return f"<StandardCollection {self.name}>"

    def _validate_id(self, doc_id: str) -> str:
        """Check the collection name in the document ID.

        Args:
            doc_id (str): Document ID.

        Returns:
            str: Verified document ID.

        Raises:
            DocumentParseError: On bad collection name.
        """
        if not doc_id.startswith(self._id_prefix):
            raise DocumentParseError(f'Bad collection name in document ID "{doc_id}"')
        return doc_id

    def _extract_id(self, body: Json) -> str:
        """Extract the document ID from document body.

        Args:
            body (dict): Document body.

        Returns:
            str: Document ID.

        Raises:
            DocumentParseError: On missing ID and key.
        """
        try:
            if "_id" in body:
                return self._validate_id(body["_id"])
            else:
                key: str = body["_key"]
                return self._id_prefix + key
        except KeyError:
            raise DocumentParseError('Field "_key" or "_id" required')

    def _prep_from_doc(
        self,
        document: str | Json,
        rev: Optional[str] = None,
        check_rev: bool = False,
    ) -> Tuple[str, Json]:
        """Prepare document ID, body and request headers before a query.

        Args:
            document (str | dict): Document ID, key or body.
            rev (str | None): Document revision.
            check_rev (bool): Whether to check the revision.

        Returns:
            Document ID and request headers.

        Raises:
            DocumentParseError: On missing ID and key.
            TypeError: On bad document type.
        """
        if isinstance(document, dict):
            doc_id = self._extract_id(document)
            rev = rev or document.get("_rev")
        elif isinstance(document, str):
            if "/" in document:
                doc_id = self._validate_id(document)
            else:
                doc_id = self._id_prefix + document
        else:
            raise TypeError("Document must be str or a dict")

        if not check_rev or rev is None:
            return doc_id, {}
        else:
            return doc_id, {"If-Match": rev}

    @property
    def name(self) -> str:
        """Return the name of the collection.

        Returns:
            str: Collection name.
        """
        return self._name


class StandardCollection(Collection[T, U, V]):
    """Standard collection API wrapper.

    Args:
        executor (ApiExecutor): API executor.
        name (str): Collection name
        doc_serializer (Serializer): Document serializer.
        doc_deserializer (Deserializer): Document deserializer.
    """

    def __init__(
        self,
        executor: ApiExecutor,
        name: str,
        doc_serializer: Serializer[T],
        doc_deserializer: Deserializer[U, V],
    ) -> None:
        super().__init__(executor, name, doc_serializer, doc_deserializer)

    async def get(
        self,
        document: str | Json,
        rev: Optional[str] = None,
        check_rev: bool = True,
        allow_dirty_read: bool = False,
    ) -> Result[Optional[U]]:
        """Return a document.

        Args:
            document (str | dict): Document ID, key or body.
            Document body must contain the "_id" or "_key" field.
            rev (str | None): Expected document revision. Overrides the
            value of "_rev" field in **document** if present.
            check_rev (bool): If set to True, revision of **document** (if given)
            is compared against the revision of target document.
            allow_dirty_read (bool):  Allow reads from followers in a cluster.

        Returns:
            Document or None if not found.

        Raises:
            DocumentRevisionError: If the revision is incorrect.
            DocumentGetError: If retrieval fails.
        """
        handle, headers = self._prep_from_doc(document, rev, check_rev)

        if allow_dirty_read:
            headers["x-arango-allow-dirty-read"] = "true"

        request = Request(
            method=Method.GET,
            endpoint=f"/_api/document/{handle}",
            headers=headers,
        )

        def response_handler(resp: Response) -> Optional[U]:
            if resp.is_success:
                return self._doc_deserializer.loads(resp.raw_body)
            elif resp.status_code == HTTP_NOT_FOUND:
                return None
            elif resp.status_code == HTTP_PRECONDITION_FAILED:
                raise DocumentRevisionError(resp, request)
            else:
                raise DocumentGetError(resp, request)

        return await self._executor.execute(request, response_handler)
