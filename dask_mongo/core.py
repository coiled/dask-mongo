import pymongo
from pymongo import MongoClient
import json 
from typing import Dict
import pandas as pd

import dask
import dask.dataframe as dd
from dask import delayed
#write to mongo db 



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
    print(type(df))
    documents = df.to_dict("records") 

    mongo_client = MongoClient(**connection_args)

    db = mongo_client.get_database(database)

    db[coll].insert_many(documents)


def to_mongo(
    df,
    *,
    connection_args: Dict, 
    database: str, #name of data base
    coll: str,    #name of collection
):

    mongo_client = MongoClient(**connection_args)

    check_db_exists(mongo_client, database)

    dask.compute(
        [write_mongo(partition, connection_args, database, coll) for partition in df.to_delayed()]
    )

    