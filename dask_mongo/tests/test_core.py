import subprocess

import dask.dataframe as dd
import pandas as pd
import pymongo
import pytest

from dask_mongo import to_mongo


@pytest.fixture
def mongo_client(tmp_path):
    port = 27016
    with subprocess.Popen(
        ["mongod", f"--dbpath={str(tmp_path)}", f"--port={port}"]
    ) as proc:
        client = pymongo.MongoClient(port=port)
        yield client
        client.close()
        proc.terminate()


def test_to_mongo(mongo_client):
    df = pd.DataFrame({"a": range(10), "b": range(10, 20)})
    ddf = dd.from_pandas(df, npartitions=3)
    assert mongo_client.address[1] == 27016
    to_mongo(
        ddf,
        connection_args={
            "host": mongo_client.address[0],
            "port": mongo_client.address[1],
        },
        database="foo",
        coll="bar",
    )

    assert "foo" in mongo_client.list_database_names()
    assert ["bar"] == mongo_client["foo"].list_collection_names()
