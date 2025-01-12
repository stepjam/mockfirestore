from collections.abc import AsyncIterator
from typing import Any

from mockfirestore._helpers import consume_async_iterable
from mockfirestore.document import DocumentSnapshot
from mockfirestore.query import Query


class AsyncQuery(Query):
    def __init__(
        self,
        parent: "AsyncCollectionReference",  # ruff: noqa: F821
        projection=None,
        field_filters=(),
        orders=(),
        limit=None,
        offset=None,
        start_at=None,
        end_at=None,
        all_descendants=False,
    ) -> None:
        super().__init__(
            parent=parent,
            projection=projection,
            field_filters=field_filters,
            orders=orders,
            limit=limit,
            offset=offset,
            start_at=start_at,
            end_at=end_at,
            all_descendants=all_descendants,
        )

    async def stream(self, transaction=None) -> AsyncIterator[DocumentSnapshot]:
        doc_snapshots = await consume_async_iterable(self.parent.stream())
        doc_snapshots = super()._process_field_filters(doc_snapshots)
        doc_snapshots = super()._process_pagination(doc_snapshots)
        for doc_snapshot in doc_snapshots:
            yield doc_snapshot

    async def get(self, transaction=None) -> list[DocumentSnapshot]:
        return await consume_async_iterable(self.stream())

    def where(
        self,
        field: str | None = None,
        op: str | None = None,
        value: Any | None = None,
        *,
        filter: Any | None = None
    ) -> "AsyncQuery":
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
