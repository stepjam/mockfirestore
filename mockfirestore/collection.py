from collections.abc import Iterable, Sequence
from typing import Any

from mockfirestore import AlreadyExists
from mockfirestore._helpers import (
    Store,
    Timestamp,
    generate_random_string,
    get_by_path,
    set_by_path,
)
from mockfirestore.document import DocumentReference, DocumentSnapshot
from mockfirestore.query import Query


class CollectionReference:
    def __init__(
        self, data: Store, path: list[str], parent: DocumentReference | None = None
    ) -> None:
        self._data = data
        self._path = path
        self.parent = parent

    def document(self, document_id: str | None = None) -> DocumentReference:
        collection = get_by_path(self._data, self._path)
        if document_id is None:
            document_id = generate_random_string()
        new_path = self._path + [document_id]
        if document_id not in collection:
            set_by_path(self._data, new_path, {})
        return DocumentReference(self._data, new_path, parent=self)

    def get(self) -> list[DocumentSnapshot]:
        return list(self.stream())

    def add(
        self, document_data: dict, document_id: str = None
    ) -> tuple[Timestamp, DocumentReference]:
        if document_id is None:
            document_id = document_data.get("id", generate_random_string())
        collection = get_by_path(self._data, self._path)
        new_path = self._path + [document_id]
        if document_id in collection:
            raise AlreadyExists(f"Document already exists: {new_path}")
        doc_ref = DocumentReference(self._data, new_path, parent=self)
        doc_ref.set(document_data)
        timestamp = Timestamp.from_now()
        return timestamp, doc_ref

    def where(
        self,
        field: str | None = None,
        op: str | None = None,
        value: Any | None = None,
        *,
        filter: Any | None = None,
    ) -> Query:
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

            from mockfirestore.field_filter import CompositeFilter, FieldFilter

            if isinstance(filter, (FieldFilter, CompositeFilter)):
                if isinstance(filter, FieldFilter):
                    field_filters = [filter.get_filter_tuple()]
                else:  # CompositeFilter
                    field_filters = filter.get_filter_tuples()
                return Query(self, field_filters=field_filters)

        if any(arg is None for arg in (field, op, value)):
            raise ValueError("When not using 'filter', all arguments must be provided")

        return Query(self, field_filters=[(field, op, value)])

    def order_by(self, key: str, direction: str | None = None) -> Query:
        query = Query(self, orders=[(key, direction)])
        return query

    def limit(self, limit_amount: int) -> Query:
        query = Query(self, limit=limit_amount)
        return query

    def offset(self, offset: int) -> Query:
        query = Query(self, offset=offset)
        return query

    def start_at(self, document_fields_or_snapshot: dict | DocumentSnapshot) -> Query:
        query = Query(self, start_at=(document_fields_or_snapshot, True))
        return query

    def start_after(
        self, document_fields_or_snapshot: dict | DocumentSnapshot
    ) -> Query:
        query = Query(self, start_at=(document_fields_or_snapshot, False))
        return query

    def end_at(self, document_fields_or_snapshot: dict | DocumentSnapshot) -> Query:
        query = Query(self, end_at=(document_fields_or_snapshot, True))
        return query

    def end_before(self, document_fields_or_snapshot: dict | DocumentSnapshot) -> Query:
        query = Query(self, end_at=(document_fields_or_snapshot, False))
        return query

    def list_documents(
        self, page_size: int | None = None
    ) -> Sequence[DocumentReference]:
        docs = []
        for key in get_by_path(self._data, self._path):
            docs.append(self.document(key))
        return docs

    def stream(self, transaction=None) -> Iterable[DocumentSnapshot]:
        for key in sorted(get_by_path(self._data, self._path)):
            doc_snapshot = self.document(key).get()
            yield doc_snapshot
