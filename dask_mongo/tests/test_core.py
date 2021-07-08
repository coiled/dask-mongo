import subprocess

import dask.dataframe as dd
import pandas as pd
import pymongo
import pytest
from dask.dataframe.utils import assert_eq

from dask_mongo import to_mongo


@pytest.fixture
def connection_args(tmp_path):
    port = 27016
    with subprocess.Popen(
        ["mongod", f"--dbpath={str(tmp_path)}", f"--port={port}"]
    ) as proc:
        connection_args = {
            "host": "localhost",
            "port": port,
        }
        yield connection_args
        proc.terminate()


def test_to_mongo(connection_args):
    df = pd.DataFrame({"a": range(10), "b": range(10, 20)})
    ddf = dd.from_pandas(df, npartitions=3)

    with pymongo.MongoClient(**connection_args) as mongo_client:
        db_name = "test-db"
        assert db_name not in mongo_client.list_database_names()
        coll_name = "test-collection"

        to_mongo(
            ddf,
            connection_args=connection_args,
            database=db_name,
            coll=coll_name,
        )

        assert db_name in mongo_client.list_database_names()
        assert [coll_name] == mongo_client[db_name].list_collection_names()

        result = pd.DataFrame.from_records(mongo_client[db_name][coll_name].find())
        result = result.drop(columns=["_id"]).sort_values(by="a").reset_index(drop=True)
        assert_eq(ddf, result)
