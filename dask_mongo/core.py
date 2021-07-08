from typing import Dict

import dask
import pandas as pd
import pymongo
from dask import delayed


def check_db_exists(client, db):
    db_names = client.list_database_names()

    try:
        db in db_names
    except ValueError:
        print(f"The database {db} does not exists")
        return


@delayed
def write_mongo(
    df: pd.DataFrame,
    connection_args,
    database,
    coll,
):
    with pymongo.MongoClient(**connection_args) as mongo_client:
        db = mongo_client.get_database(database)
        db[coll].insert_many(df.to_dict("records"))


def to_mongo(
    df,
    *,
    connection_args: Dict,
    database: str,
    coll: str,
    compute_options: Dict = None,
):

    with pymongo.MongoClient(**connection_args) as mongo_client:
        check_db_exists(mongo_client, database)

    partitions = [
        write_mongo(partition, connection_args, database, coll)
        for partition in df.to_delayed()
    ]

    if compute_options is None:
        compute_options = {}

    from distributed import get_client

    try:
        client = get_client()
    except ValueError:
        # Using single-machine scheduler
        dask.compute(partitions, **compute_options)
    else:
        return client.compute(partitions, **compute_options)
