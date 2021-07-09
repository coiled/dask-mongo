from typing import Dict

import dask
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


def read_mongo(
    connection_args: Dict,
    database: str,
    collection: str,
    chunk_size: int,
    match: Dict = {},
):
    with pymongo.MongoClient(**connection_args) as mongo_client:
        db = mongo_client.get_database(database)

        n_docs = next(
            (
                db[collection].aggregate(
                    [
                        {"$match": match},
                        {"$count": "count"},
                    ]
                )
            )
        )["count"]

        n_chunks = chunk_size // n_docs + bool(chunk_size % n_docs)

        chunks_ids = list(
            db["collection"].aggregate(
                [
                    {"$match": match},
                    {"$bucketAuto": {"groupBy": "$_id", "buckets": n_chunks}},
                ],
                allowDiskUse=True,
            )
        )

        chunks_list = []

        # this is what we should do using dask compute
        for chunk in chunks_ids:
            id_min = chunk["_id"]["min"]
            id_max = chunk["_id"]["max"]

            chunks_list.append(
                list(
                    db["collection"].aggregate(
                        [
                            {"$match": match},
                            {"$match": {"_id": {"$gte": id_min, "$lt": id_max}}},
                        ]
                    )
                )
            )

        # not sure how to go about this part
        # how to make a pd dataframe one partition on dask dataframe,
        pd.DataFrame.from_records(chunks_list[0])
