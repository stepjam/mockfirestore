from collections.abc import Callable, Iterable, Iterator
from itertools import islice, tee
from typing import Any

from mockfirestore._helpers import T
from mockfirestore.document import DocumentSnapshot


class Query:
    def __init__(
        self,
        parent: "CollectionReference",  # ruff: noqa: F821
        projection=None,
        field_filters=(),
        orders=(),
        limit=None,
        offset=None,
        start_at=None,
        end_at=None,
        all_descendants=False,
    ) -> None:
        self.parent = parent
        self.projection = projection
        self._field_filters = []
        self.orders = list(orders)
        self._limit = limit
        self._offset = offset
        self._start_at = start_at
        self._end_at = end_at
        self.all_descendants = all_descendants

        if field_filters:
            if isinstance(field_filters, (list, tuple)):
                for field_filter in field_filters:
                    self._add_field_filter(*field_filter)
            else:
                # Handle new filter object
                from mockfirestore.field_filter import CompositeFilter, FieldFilter

                if isinstance(field_filters, (FieldFilter, CompositeFilter)):
                    if isinstance(field_filters, FieldFilter):
                        self._add_field_filter(*field_filters.get_filter_tuple())
                    else:  # CompositeFilter
                        for filter_tuple in field_filters.get_filter_tuples():
                            self._add_field_filter(*filter_tuple)

    def where(
        self,
        field: str | None = None,
        op: str | None = None,
        value: Any | None = None,
        *,
        filter: Any | None = None
    ) -> "Query":
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
                    self._add_field_filter(*filter.get_filter_tuple())
                else:  # CompositeFilter
                    for filter_tuple in filter.get_filter_tuples():
                        self._add_field_filter(*filter_tuple)
            return self

        if any(arg is None for arg in (field, op, value)):
            raise ValueError("When not using 'filter', all arguments must be provided")

        self._add_field_filter(field, op, value)
        return self

    def _process_pagination(self, doc_snapshots: Iterator[DocumentSnapshot]):
        if self.orders:
            for key, direction in self.orders:
                doc_snapshots = sorted(
                    doc_snapshots,
                    key=lambda doc: doc.to_dict()[key],
                    reverse=direction == "DESCENDING",
                )
        if self._start_at:
            document_fields_or_snapshot, before = self._start_at
            doc_snapshots = self._apply_cursor(
                document_fields_or_snapshot, doc_snapshots, before, True
            )

        if self._end_at:
            document_fields_or_snapshot, before = self._end_at
            doc_snapshots = self._apply_cursor(
                document_fields_or_snapshot, doc_snapshots, before, False
            )

        if self._offset:
            doc_snapshots = islice(doc_snapshots, self._offset, None)

        if self._limit:
            doc_snapshots = islice(doc_snapshots, self._limit)

        return iter(doc_snapshots)

    def _process_field_filters(
        self, doc_snapshots: Iterator[DocumentSnapshot]
    ) -> Iterable[DocumentSnapshot]:
        for field, compare, value in self._field_filters:
            doc_snapshots = [
                doc_snapshot
                for doc_snapshot in doc_snapshots
                if compare(doc_snapshot._get_by_field_path(field), value)
            ]
        return doc_snapshots

    def stream(self, transaction=None) -> Iterator[DocumentSnapshot]:
        doc_snapshots = (doc for doc in self.parent.stream() if doc.exists)
        doc_snapshots = self._process_field_filters(doc_snapshots)
        return self._process_pagination(doc_snapshots)

    def get(self, transaction=None) -> list[DocumentSnapshot]:
        return list(self.stream())

    def _add_field_filter(self, field: str, op: str, value: Any):
        compare = self._compare_func(op)
        self._field_filters.append((field, compare, value))

    def order_by(self, key: str, direction: str | None = "ASCENDING") -> "Query":
        self.orders.append((key, direction))
        return self

    def limit(self, limit_amount: int) -> "Query":
        self._limit = limit_amount
        return self

    def offset(self, offset_amount: int) -> "Query":
        self._offset = offset_amount
        return self

    def start_at(self, document_fields_or_snapshot: dict | DocumentSnapshot) -> "Query":
        self._start_at = (document_fields_or_snapshot, True)
        return self

    def start_after(
        self, document_fields_or_snapshot: dict | DocumentSnapshot
    ) -> "Query":
        self._start_at = (document_fields_or_snapshot, False)
        return self

    def end_at(self, document_fields_or_snapshot: dict | DocumentSnapshot) -> "Query":
        self._end_at = (document_fields_or_snapshot, True)
        return self

    def end_before(
        self, document_fields_or_snapshot: dict | DocumentSnapshot
    ) -> "Query":
        self._end_at = (document_fields_or_snapshot, False)
        return self

    def _apply_cursor(
        self,
        document_fields_or_snapshot: dict | DocumentSnapshot,
        doc_snapshot: Iterator[DocumentSnapshot],
        before: bool,
        start: bool,
    ) -> Iterator[DocumentSnapshot]:
        docs, doc_snapshot = tee(doc_snapshot)
        for idx, doc in enumerate(doc_snapshot):
            index = None
            if isinstance(document_fields_or_snapshot, dict):
                for k, v in document_fields_or_snapshot.items():
                    if doc.to_dict().get(k, None) == v:
                        index = idx
                    else:
                        index = None
                        break
            elif isinstance(document_fields_or_snapshot, DocumentSnapshot):
                if doc.id == document_fields_or_snapshot.id:
                    index = idx
            if index is not None:
                if before and start:
                    return islice(docs, index, None, None)
                elif not before and start:
                    return islice(docs, index + 1, None, None)
                elif before and not start:
                    return islice(docs, 0, index + 1, None)
                elif not before and not start:
                    return islice(docs, 0, index, None)

    def _compare_func(self, op: str) -> Callable[[T, T], bool]:
        f = None
        if op == "==":

            def f(x, y):
                return x == y

        elif op == "!=":

            def f(x, y):
                return x != y

        elif op == "<":

            def f(x, y):
                return x < y

        elif op == "<=":

            def f(x, y):
                return x <= y

        elif op == ">":

            def f(x, y):
                return x > y

        elif op == ">=":

            def f(x, y):
                return x >= y

        elif op == "in":

            def f(x, y):
                return x in y

        elif op == "array_contains":

            def f(x, y):
                return y in x

        elif op == "array_contains_any":

            def f(x, y):
                return any([(val in y) for val in x])

        def _comp_func(x, y):
            if x is None:
                return False
            if y is None:
                return True
            return f(x, y)

        return _comp_func
