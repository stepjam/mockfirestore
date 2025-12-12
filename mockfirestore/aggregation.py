from typing import TYPE_CHECKING, List, Optional, Iterable, Union
import statistics

from mockfirestore._helpers import AggregationResult
if TYPE_CHECKING:
    from mockfirestore.query import Query


class AggregationQuery:
    def __init__(self, query: "Query", field: Optional[Union[str, Iterable[str]]] = None,
                 alias: Optional[str] = None, aggregation_type: str = "count"):
        self._query = query
        self._field = field
        self._alias = alias
        self._aggregation_type = aggregation_type

    def get(self, transaction=None) -> List[List[AggregationResult]]:
        docs = self._query.stream(transaction=transaction)
        if self._aggregation_type == "count":
            value = len(list(docs))
        elif self._aggregation_type == "sum":
            if self._field is None:
                raise ValueError("Field must be specified for sum aggregation.")
            field_str = ".".join(self._field) if isinstance(self._field, Iterable) else self._field
            value = sum(doc._get_by_field_path(field_str) or 0 for doc in docs)
        elif self._aggregation_type == "avg":
            if self._field is None:
                raise ValueError("Field must be specified for average aggregation.")
            field_str = ".".join(self._field) if isinstance(self._field, Iterable) else self._field
            values = [doc._get_by_field_path(field_str) for doc in docs if doc._get_by_field_path(field_str) is not None]
            value = statistics.mean(values) if values else 0
        else:
            raise NotImplementedError(f"Aggregation type '{self._aggregation_type}' is not implemented.")

        return [[AggregationResult(value, alias=self._alias)]]
