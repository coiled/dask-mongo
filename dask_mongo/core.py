from __future__ import annotations

from math import ceil

import pymongo
from bson import ObjectId
from dask.bag import Bag
from dask.base import tokenize
from dask.delayed import Delayed
from dask.graph_manipulation import checkpoint


def write_mongo(
    values: list[dict],
    connection_args: dict,
    database: str,
    collection: str,
) -> None:
    with pymongo.MongoClient(**connection_args) as mongo_client:
        coll = mongo_client[database][collection]
        # `insert_many` will mutate its input by inserting a "_id" entry.
        # This can lead to confusing results; pass copies to it to preserve the input.
        values = [v.copy() for v in values]
        coll.insert_many(values)


def to_mongo(
    bag: Bag,
    connection_args: dict,
    database: str,
    collection: str,
) -> Delayed:
    """Write a Dask Bag to a Mongo database.

    Parameters
    ----------
    bag:
      Dask Bag to write into the database.
    connection_args:
      Connection arguments to pass to ``MongoClient``.
    database:
      Name of the database to write to. If it does not exists it will be created.
    collection:
      Name of the collection within the database to write to.
      If it does not exists it will be created.
    Returns
    -------
    dask.delayed
    """
    partials = bag.map_partitions(write_mongo, connection_args, database, collection)
    return checkpoint(partials)


def fetch_mongo(
    connection_args: dict,
    database: str,
    collection: str,
    match: dict,
    id_min: ObjectId,
    id_max: ObjectId,
    include_last: bool,
):
    with pymongo.MongoClient(**connection_args) as mongo_client:
        coll = mongo_client[database][collection]
        return list(
            coll.aggregate(
                [
                    {"$match": match},
                    {
                        "$match": {
                            "_id": {
                                "$gte": id_min,
                                "$lte" if include_last else "$lt": id_max,
                            }
                        }
                    },
                ]
            )
        )


def read_mongo(
    connection_args: dict,
    database: str,
    collection: str,
    chunksize: int,
    match: dict = None,
):
    """Read data from a Mongo database into a Dask Bag.

    Parameters
    ----------
    connection_args:
      Connection arguments to pass to ``MongoClient``
    database:
      Name of the database to read from
    collection:
      Name of the collection within the database to read from
    chunksize:
      Number of elements desired per partition.
    match:
      MongoDB match query, used to filter the documents in the collection. If omitted,
      this function will load all the documents in the collection.
    """
    if not match:
        match = {}

    with pymongo.MongoClient(**connection_args) as mongo_client:
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

    common_args = (connection_args, database, collection, match)
    name = "read_mongo-" + tokenize(common_args)
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
