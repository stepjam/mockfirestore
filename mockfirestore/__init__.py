# Try to import gcloud exceptions
try:
    from google.api_core.exceptions import (
        AlreadyExists,
        ClientError,
        Conflict,
        NotFound,
    )
except ImportError:
    from mockfirestore.exceptions import ClientError, Conflict, NotFound, AlreadyExists

from mockfirestore._helpers import Timestamp
from mockfirestore.async_client import AsyncMockFirestore
from mockfirestore.async_collection import AsyncCollectionReference
from mockfirestore.async_document import AsyncDocumentReference
from mockfirestore.async_query import AsyncQuery
from mockfirestore.async_transaction import AsyncTransaction
from mockfirestore.client import MockFirestore
from mockfirestore.collection import CollectionReference
from mockfirestore.document import DocumentReference, DocumentSnapshot
from mockfirestore.field_filter import (
    CompositeFilter,
    FieldFilter,
    and_filter,
    create_filter,
    or_filter,
)
from mockfirestore.query import Query
from mockfirestore.transaction import Transaction

__all__ = [
    "MockFirestore",
    "DocumentSnapshot",
    "DocumentReference",
    "CollectionReference",
    "Query",
    "Timestamp",
    "Transaction",
    "AsyncMockFirestore",
    "AsyncDocumentReference",
    "AsyncCollectionReference",
    "AsyncQuery",
    "AsyncTransaction",
    "FieldFilter",
    "CompositeFilter",
    "create_filter",
    "and_filter",
    "or_filter",
    "ClientError",
    "Conflict",
    "NotFound",
    "AlreadyExists",
]
