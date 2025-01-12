import operator
import random
import string
from collections.abc import AsyncIterable, Iterator, Sequence
from datetime import datetime as dt
from functools import reduce
from typing import Any, TypeVar

T = TypeVar("T")
KeyValuePair = tuple[str, dict[str, Any]]
Document = dict[str, Any]
Collection = dict[str, Document]
Store = dict[str, Collection]


def get_by_path(
    data: dict[str, T], path: Sequence[str], create_nested: bool = False
) -> T:
    """Access a nested object in root by item sequence."""

    def get_or_create(a, b):
        if b not in a:
            a[b] = {}
        return a[b]

    if create_nested:
        return reduce(get_or_create, path, data)
    else:
        return reduce(operator.getitem, path, data)


def set_by_path(
    data: dict[str, T], path: Sequence[str], value: T, create_nested: bool = True
):
    """Set a value in a nested object in root by item sequence."""
    get_by_path(data, path[:-1], create_nested=True)[path[-1]] = value


def delete_by_path(data: dict[str, T], path: Sequence[str]):
    """Delete a value in a nested object in root by item sequence."""
    del get_by_path(data, path[:-1])[path[-1]]


def generate_random_string():
    return "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(20)
    )


class Timestamp:
    """
    Imitates some properties of `google.protobuf.timestamp_pb2.Timestamp`
    """

    def __init__(self, timestamp: float):
        self._timestamp = timestamp

    @classmethod
    def from_now(cls):
        timestamp = dt.now().timestamp()
        return cls(timestamp)

    @property
    def seconds(self):
        return str(self._timestamp).split(".")[0]

    @property
    def nanos(self):
        return str(self._timestamp).split(".")[1]


def get_document_iterator(
    document: dict[str, Any], prefix: str = ""
) -> Iterator[tuple[str, Any]]:
    """
    :returns: (dot-delimited path, value,)
    """
    for key, value in document.items():
        if isinstance(value, dict):
            yield from get_document_iterator(value, prefix=key)

        if not prefix:
            yield key, value
        else:
            yield f"{prefix}.{key}", value


async def consume_async_iterable(iterable: AsyncIterable[T]) -> list[T]:
    return [item async for item in iterable if item.exists]
