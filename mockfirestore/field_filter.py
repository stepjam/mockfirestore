from dataclasses import dataclass
from typing import Any, Union


@dataclass
class FieldFilter:
    """
    Represents a field filter in a Firestore query.
    """

    field_path: str
    op_string: str
    value: Any

    def __init__(self, field_path: str, op_string: str, value: Any):
        self.field_path = field_path
        self.op_string = op_string
        self.value = value

    def get_filter_tuple(self) -> tuple[str, str, Any]:
        """
        Returns a tuple of (field_path, op_string, value)
        for compatibility with existing code.
        """
        return (self.field_path, self.op_string, self.value)


class CompositeFilter:
    """
    Represents a composite filter that combines multiple filters with AND/OR operations.
    """

    def __init__(
        self,
        filters: list[Union[FieldFilter, "CompositeFilter"]],
        operator: str = "AND",
    ):
        self.filters = filters
        self.operator = operator

    def get_filter_tuples(self) -> list[tuple[str, str, Any]]:
        """
        Flattens the composite filter into a list of filter tuples.
        For AND operations, this works with the existing implementation.
        For OR operations, this will need additional handling in the Query class.
        """
        result = []
        for filter_obj in self.filters:
            if isinstance(filter_obj, FieldFilter):
                result.append(filter_obj.get_filter_tuple())
            elif isinstance(filter_obj, CompositeFilter):
                result.extend(filter_obj.get_filter_tuples())
        return result


def create_filter(
    field_path_or_filter: str | FieldFilter,
    op_string: str | None = None,
    value: Any | None = None,
) -> FieldFilter:
    """
    Creates a new field filter. Supports both new and old syntax:
    - New: create_filter(FieldFilter("field", ">", value))
    - Old: create_filter("field", ">", value)
    """
    if isinstance(field_path_or_filter, FieldFilter):
        if op_string is not None or value is not None:
            raise ValueError(
                "When using FieldFilter, no additional arguments should be provided"
            )
        return field_path_or_filter

    if op_string is None or value is None:
        raise ValueError(
            "When using field path string, operator and value must be provided"
        )

    return FieldFilter(field_path_or_filter, op_string, value)


def and_filter(*filters: FieldFilter | CompositeFilter) -> CompositeFilter:
    """Creates a composite AND filter from the given filters."""
    return CompositeFilter(list(filters), "AND")


def or_filter(*filters: FieldFilter | CompositeFilter) -> CompositeFilter:
    """Creates a composite OR filter from the given filters."""
    return CompositeFilter(list(filters), "OR")
