from math import ceil
from typing import Dict

import dask
import dask.dataframe as dd
import pandas as pd
import pymongo
from dask import delayed
from distributed import get_client


@delayed
def write_mongo(
    df: pd.DataFrame,
    connection_args,
    database,
    collection,
):
    with pymongo.MongoClient(**connection_args) as mongo_client:
        db = mongo_client.get_database(database)
        db[collection].insert_many(df.to_dict("records"))


def to_mongo(
    df,
    *,
    connection_args: Dict,
    database: str,
    collection: str,
    compute_options: Dict = None,
):

    if compute_options is None:
        compute_options = {}

    partitions = [
        write_mongo(partition, connection_args, database, collection)
        for partition in df.to_delayed()
    ]

    try:
        client = get_client()
    except ValueError:
        # Using single-machine scheduler
        dask.compute(partitions, **compute_options)
    else:
        return client.compute(partitions, **compute_options)


@delayed
def fetch_mongo(
    connection_args,
    database,
    collection,
    id_min,
    id_max,
    match,
    include_last=False,
):
    with pymongo.MongoClient(**connection_args) as mongo_client:
        db = mongo_client.get_database(database)

        results = list(
            db[collection].aggregate(
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

    return pd.DataFrame.from_records(results)


def read_mongo(
    connection_args: Dict,
    database: str,
    collection: str,
    chunksize: int,
    match: Dict = {},
):
    with pymongo.MongoClient(**connection_args) as mongo_client:
        db = mongo_client.get_database(database)

        nrows = next(
            (
                db[collection].aggregate(
                    [
                        {"$match": match},
                        {"$count": "count"},
                    ]
                )
            )
        )["count"]

        npartitions = int(ceil(nrows / chunksize))

        partitions_ids = list(
            db[collection].aggregate(
                [
                    {"$match": match},
                    {"$bucketAuto": {"groupBy": "$_id", "buckets": npartitions}},
                ],
                allowDiskUse=True,
            )
        )
        meta = {k: type(v) for k, v in db[collection].find_one().items()}
        meta["_id"] = object

    partitions = [
        fetch_mongo(
            connection_args,
            database,
            collection,
            partition["_id"]["min"],
            partition["_id"]["max"],
            match,
            include_last=idx == len(partitions_ids) - 1,
        )
        for idx, partition in enumerate(partitions_ids)
    ]

    return dd.from_delayed(partitions, meta=meta)
