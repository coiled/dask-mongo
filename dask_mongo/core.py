from __future__ import annotations

import atexit
import weakref
from collections.abc import Hashable, Mapping
from copy import copy
from functools import lru_cache
from math import ceil
from typing import Any

import pymongo
from bson import ObjectId
from dask.bag import Bag
from dask.base import tokenize
from dask.graph_manipulation import checkpoint

from ._version import __version__

appname = f"dask-mongo/{__version__}"

_CACHE_SIZE = 10
_CLIENTS = weakref.WeakValueDictionary({})


def _recursive_tupling(item):
    if isinstance(item, list):
        return tuple([_recursive_tupling(i) for i in item])
    if isinstance(item, Mapping):
        return tuple(
            [(_recursive_tupling(k), _recursive_tupling(v)) for k, v in item.items()]
        )
    elif isinstance(item, Hashable):
        return hash(item)
    else:
        return item


class HashableKwargs(dict):
    def __hash__(self):
        return hash(
            frozenset(
                [
                    (_recursive_tupling(k), _recursive_tupling(v))
                    for k, v in self.items()
                ]
            )
        )


@lru_cache(_CACHE_SIZE, typed=True)
def _cache_inner(kwargs):
    return pymongo.MongoClient(appname=appname, **kwargs)


@atexit.register
def _close_clients():
    global _CLIENTS
    for func in _CLIENTS.values():
        ref = func()
        if ref:
            ref()


def _get_client(kwargs):
    frozen_kwargs = HashableKwargs(kwargs)
    client = _cache_inner(frozen_kwargs)
    _CLIENTS[frozen_kwargs] = weakref.WeakMethod(client.close)
    return client


def write_mongo(
    values: list[dict],
    connection_kwargs: dict[str, Any],
    database: str,
    collection: str,
) -> None:
    mongo_client = _get_client(connection_kwargs)
    coll = mongo_client[database][collection]
    # `insert_many` will mutate its input by inserting a "_id" entry.
    # This can lead to confusing results; pass copies to it to preserve the input.
    values = [copy(v) for v in values]
    coll.insert_many(values)


def to_mongo(
    bag: Bag,
    database: str,
    collection: str,
    *,
    connection_kwargs: dict[str, Any] = None,
    compute: bool = True,
    compute_kwargs: dict[str, Any] = None,
) -> Any:
    """Write a Dask Bag to a Mongo database.

    Parameters
    ----------
    bag:
      Dask Bag to write into the database.
    database : str
      Name of the database to write to. If it does not exists it will be created.
    collection : str
      Name of the collection within the database to write to.
      If it does not exists it will be created.
    connection_kwargs : dict
      Arguments to pass to ``MongoClient``.
    compute : bool, optional
        If true, immediately executes. If False, returns a delayed
        object, which can be computed at a later time.
    compute_kwargs : dict, optional
        Options to be passed in to the compute method
    Returns
    -------
    If compute=True, block until computation is done, then return None.
    If compute=False, immediately return a dask.delayed object.
    """
    partials = bag.map_partitions(
        write_mongo, connection_kwargs or {}, database, collection
    )
    collect = checkpoint(partials)
    if compute:
        return collect.compute(**compute_kwargs or {})
    else:
        return collect


def fetch_mongo(
    connection_kwargs: dict[str, Any],
    database: str,
    collection: str,
    match: dict[str, Any],
    id_min: ObjectId,
    id_max: ObjectId,
    include_last: bool,
) -> list[dict[str, Any]]:
    match2 = {"_id": {"$gte": id_min, "$lte" if include_last else "$lt": id_max}}
    mongo_client = _get_client(connection_kwargs)
    coll = mongo_client[database][collection]
    return list(coll.aggregate([{"$match": match}, {"$match": match2}]))


def read_mongo(
    database: str,
    collection: str,
    chunksize: int,
    *,
    connection_kwargs: dict[str, Any] = None,
    match: dict[str, Any] = None,
):
    """Read data from a Mongo database into a Dask Bag.

    Parameters
    ----------
    database:
      Name of the database to read from
    collection:
      Name of the collection within the database to read from
    chunksize:
      Number of elements desired per partition.
    connection_kwargs:
      Connection arguments to pass to ``MongoClient``
    match:
      MongoDB match query, used to filter the documents in the collection. If omitted,
      this function will load all the documents in the collection.
    """
    if not connection_kwargs:
        connection_kwargs = {}
    if not match:
        match = {}

    mongo_client = _get_client(connection_kwargs)
    coll = mongo_client[database][collection]

    nrows = next(
        (
            coll.aggregate(
                [
                    {"$match": match},
                    {"$count": "count"},
                ]
            )
        )
    )["count"]

    npartitions = int(ceil(nrows / chunksize))

    partitions_ids = list(
        coll.aggregate(
            [
                {"$match": match},
                {"$bucketAuto": {"groupBy": "$_id", "buckets": npartitions}},
            ],
            allowDiskUse=True,
        )
    )

    common_args = (connection_kwargs, database, collection, match)
    name = "read_mongo-" + tokenize(common_args, chunksize)
    dsk = {
        (name, i): (
            fetch_mongo,
            *common_args,
            partition["_id"]["min"],
            partition["_id"]["max"],
            i == len(partitions_ids) - 1,
        )
        for i, partition in enumerate(partitions_ids)
    }
    return Bag(dsk, name, len(partitions_ids))
