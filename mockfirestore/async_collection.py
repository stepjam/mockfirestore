from collections.abc import AsyncIterator
from typing import Any

from mockfirestore._helpers import Timestamp, get_by_path
from mockfirestore.async_document import AsyncDocumentReference
from mockfirestore.async_query import AsyncQuery
from mockfirestore.collection import CollectionReference
from mockfirestore.document import DocumentReference, DocumentSnapshot


class AsyncCollectionReference(CollectionReference):
    def document(self, document_id: str | None = None) -> AsyncDocumentReference:
        doc_ref = super().document(document_id)
        return AsyncDocumentReference(
            doc_ref._data, doc_ref._path, parent=doc_ref.parent
        )

    async def get(self) -> list[DocumentSnapshot]:
        docs = []
        async for i in self.stream():
            docs.append(i)
        return docs

    async def add(
        self, document_data: dict, document_id: str = None
    ) -> tuple[Timestamp, AsyncDocumentReference]:
        timestamp, doc_ref = super().add(document_data, document_id=document_id)
        async_doc_ref = AsyncDocumentReference(
            doc_ref._data, doc_ref._path, parent=doc_ref.parent
        )
        return timestamp, async_doc_ref

    async def list_documents(
        self, page_size: int | None = None
    ) -> AsyncIterator[DocumentReference]:
        docs = super().list_documents()
        for doc in docs:
            yield doc

    async def stream(self, transaction=None) -> AsyncIterator[DocumentSnapshot]:
        for key in sorted(get_by_path(self._data, self._path)):
            doc_snapshot = await self.document(key).get()
            yield doc_snapshot

    def where(
        self,
        field: str | None = None,
        op: str | None = None,
        value: Any | None = None,
        *,
        filter: Any | None = None
    ) -> AsyncQuery:
        """
        Supports both old and new filter syntax:
        - Old: where("field", ">", value)
        - New: where(filter=FieldFilter("field", ">", value))
        """
        if filter is not None:
            if any(arg is not None for arg in (field, op, value)):
                raise ValueError(
                    "When using 'filter', no other arguments should be provided"
                )

            from google.cloud.firestore_v1.base_query import FieldFilter

            if isinstance(filter, (FieldFilter)):
                if isinstance(filter, FieldFilter):
                    field_filters = [
                        (filter.field_path, filter.op_string, filter.value)
                    ]
                else:  # CompositeFilter
                    field_filters = filter.get_filter_tuples()
                return AsyncQuery(self, field_filters=field_filters)

        if any(arg is None for arg in (field, op, value)):
            raise ValueError("When not using 'filter', all arguments must be provided")

        return AsyncQuery(self, field_filters=[(field, op, value)])

    def order_by(self, key: str, direction: str | None = None) -> AsyncQuery:
        query = AsyncQuery(self, orders=[(key, direction)])
        return query

    def limit(self, limit_amount: int) -> AsyncQuery:
        query = AsyncQuery(self, limit=limit_amount)
        return query

    def offset(self, offset: int) -> AsyncQuery:
        query = AsyncQuery(self, offset=offset)
        return query

    def start_at(
        self, document_fields_or_snapshot: dict | DocumentSnapshot
    ) -> AsyncQuery:
        query = AsyncQuery(self, start_at=(document_fields_or_snapshot, True))
        return query

    def start_after(
        self, document_fields_or_snapshot: dict | DocumentSnapshot
    ) -> AsyncQuery:
        query = AsyncQuery(self, start_at=(document_fields_or_snapshot, False))
        return query

    def end_at(
        self, document_fields_or_snapshot: dict | DocumentSnapshot
    ) -> AsyncQuery:
        query = AsyncQuery(self, end_at=(document_fields_or_snapshot, True))
        return query

    def end_before(
        self, document_fields_or_snapshot: dict | DocumentSnapshot
    ) -> AsyncQuery:
        query = AsyncQuery(self, end_at=(document_fields_or_snapshot, False))
        return query
