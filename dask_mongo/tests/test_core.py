import random
import subprocess
from copy import deepcopy

import dask.bag as db
import pymongo
import pytest
from dask.bag.utils import assert_eq
from distributed.utils_test import cluster_fixture  # noqa: F401
from distributed.utils_test import client, gen_cluster, loop  # noqa: F401

from dask_mongo import read_mongo, to_mongo


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


def gen_data(size=10):
    records = [
        {
            "name": random.choice(["fred", "wilma", "barney", "betty"]),
            "number": random.randint(0, 100),
            "idx": i,
        }
        for i in range(size)
    ]
    return records


@gen_cluster(client=True, clean_kwargs={"threads": False})
async def test_to_mongo_distributed(c, s, a, b, connection_args):
    records = gen_data(size=10)
    npartitions = 3
    b = db.from_sequence(records, npartitions=npartitions)

    database = "test-db"
    collection = "test-collection"
    delayed_ = to_mongo(
        b,
        connection_args=connection_args,
        database=database,
        collection=collection,
    )

    with pymongo.MongoClient(**connection_args) as mongo_client:
        assert database not in mongo_client.list_database_names()
        await c.compute(delayed_)
        assert database in mongo_client.list_database_names()
        assert [collection] == mongo_client[database].list_collection_names()
        results = list(mongo_client[database][collection].find())

    # Drop "_id" and sort by "idx" for comparison
    results = [{k: v for k, v in result.items() if k != "_id"} for result in results]
    results = sorted(results, key=lambda x: x["idx"])
    assert_eq(b, results)


def test_to_mongo(connection_args):
    records = gen_data(size=10)
    npartitions = 3
    b = db.from_sequence(records, npartitions=npartitions)
    database = "test-db"
    collection = "test-collection"
    delayed_ = to_mongo(
        b,
        connection_args=connection_args,
        database=database,
        collection=collection,
    )

    with pymongo.MongoClient(**connection_args) as mongo_client:
        assert database not in mongo_client.list_database_names()
        delayed_.compute()
        assert database in mongo_client.list_database_names()
        assert [collection] == mongo_client[database].list_collection_names()

        results = list(mongo_client[database][collection].find())
    # Drop "_id" and sort by "idx" for comparison
    results = [{k: v for k, v in result.items() if k != "_id"} for result in results]
    results = sorted(results, key=lambda x: x["idx"])
    assert_eq(b, results)


def test_read_mongo(connection_args, client):
    records = gen_data(size=10)
    database = "test-db"
    collection = "test-collection"

    with pymongo.MongoClient(**connection_args) as mongo_client:
        database_ = mongo_client.get_database(database)
        database_[collection].insert_many(deepcopy(records))

    b = read_mongo(
        connection_args=connection_args,
        database=database,
        collection=collection,
        chunksize=5,
    )

    b = b.map(lambda x: {k: v for k, v in x.items() if k != "_id"})
    assert_eq(b, records)


def test_read_mongo_match(connection_args):
    records = gen_data(size=10)
    database = "test-db"
    collection = "test-collection"

    with pymongo.MongoClient(**connection_args) as mongo_client:
        database_ = mongo_client.get_database(database)
        database_[collection].insert_many(deepcopy(records))

    b = read_mongo(
        connection_args=connection_args,
        database=database,
        collection=collection,
        chunksize=5,
        match={"idx": {"$gte": 2, "$lte": 7}},
    )

    b = b.map(lambda x: {k: v for k, v in x.items() if k != "_id"})
    expected = [record for record in records if 2 <= record["idx"] <= 7]
    assert_eq(b, expected)


def test_read_mongo_chunksize(connection_args):
    records = gen_data(size=10)
    database = "test-db"
    collection = "test-collection"

    with pymongo.MongoClient(**connection_args) as mongo_client:
        database_ = mongo_client.get_database(database)
        database_[collection].insert_many(deepcopy(records))

    # divides evenly total nrows, 10/5 = 2
    b = read_mongo(
        connection_args=connection_args,
        database=database,
        collection=collection,
        chunksize=5,
    )

    assert b.npartitions == 2
    assert tuple(b.map_partitions(lambda rec: len(rec)).compute()) == (5, 5)

    # does not divides evenly total nrows, 10/3 -> 4
    b = read_mongo(
        connection_args=connection_args,
        database=database,
        collection=collection,
        chunksize=3,
    )

    assert b.npartitions == 4
    assert tuple(b.map_partitions(lambda rec: len(rec)).compute()) == (
        3,
        3,
        3,
        1,
    )
