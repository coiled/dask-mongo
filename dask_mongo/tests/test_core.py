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
def connection_kwargs(tmp_path):
    port = 27016
    with subprocess.Popen(
        ["mongod", f"--dbpath={str(tmp_path)}", f"--port={port}"]
    ) as proc:
        connection_kwargs = {
            "host": "localhost",
            "port": port,
        }
        yield connection_kwargs
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
async def test_to_mongo_distributed_async(c, s, a, b, connection_kwargs):
    records = gen_data(size=10)
    npartitions = 3
    b = db.from_sequence(records, npartitions=npartitions)

    database = "test-db"
    collection = "test-collection"
    delayed_ = to_mongo(
        b,
        database,
        collection,
        connection_kwargs=connection_kwargs,
        # DaskMethodsMixin.compute() does not work with async distributed.Client
        compute=False,
    )

    with pymongo.MongoClient(**connection_kwargs) as mongo_client:
        assert database not in mongo_client.list_database_names()
        await c.compute(delayed_)
        assert database in mongo_client.list_database_names()
        assert [collection] == mongo_client[database].list_collection_names()
        results = list(mongo_client[database][collection].find())

    # Drop "_id" and sort by "idx" for comparison
    results = [{k: v for k, v in result.items() if k != "_id"} for result in results]
    results = sorted(results, key=lambda x: x["idx"])
    assert_eq(b, results)


@pytest.mark.parametrize(
    "compute,compute_kwargs",
    [
        (False, None),
        (True, None),
        (True, dict(scheduler="sync")),
    ],
)
def test_to_mongo(connection_kwargs, compute, compute_kwargs):
    records = gen_data(size=10)
    npartitions = 3
    b = db.from_sequence(records, npartitions=npartitions)
    database = "test-db"
    collection = "test-collection"

    with pymongo.MongoClient(**connection_kwargs) as mongo_client:
        assert database not in mongo_client.list_database_names()

        out = to_mongo(
            b,
            database,
            collection,
            connection_kwargs=connection_kwargs,
            compute=compute,
            compute_kwargs=compute_kwargs,
        )
        if compute:
            assert out is None
        else:
            assert out.compute() is None

        assert database in mongo_client.list_database_names()
        assert [collection] == mongo_client[database].list_collection_names()

        results = list(mongo_client[database][collection].find())
    # Drop "_id" and sort by "idx" for comparison
    results = [{k: v for k, v in result.items() if k != "_id"} for result in results]
    results = sorted(results, key=lambda x: x["idx"])
    assert_eq(b, results)


def test_read_mongo(connection_kwargs, client):
    records = gen_data(size=10)
    database = "test-db"
    collection = "test-collection"

    with pymongo.MongoClient(**connection_kwargs) as mongo_client:
        database_ = mongo_client.get_database(database)
        database_[collection].insert_many(deepcopy(records))

    b = read_mongo(
        database,
        collection,
        chunksize=5,
        connection_kwargs=connection_kwargs,
    )

    b = b.map(lambda x: {k: v for k, v in x.items() if k != "_id"})
    assert_eq(b, records)


def test_read_mongo_match(connection_kwargs):
    records = gen_data(size=10)
    database = "test-db"
    collection = "test-collection"

    with pymongo.MongoClient(**connection_kwargs) as mongo_client:
        database_ = mongo_client.get_database(database)
        database_[collection].insert_many(deepcopy(records))

    b = read_mongo(
        database,
        collection,
        chunksize=5,
        connection_kwargs=connection_kwargs,
        match={"idx": {"$gte": 2, "$lte": 7}},
    )

    b = b.map(lambda x: {k: v for k, v in x.items() if k != "_id"})
    expected = [record for record in records if 2 <= record["idx"] <= 7]
    assert_eq(b, expected)


def test_read_mongo_chunksize(connection_kwargs):
    records = gen_data(size=10)
    database = "test-db"
    collection = "test-collection"

    with pymongo.MongoClient(**connection_kwargs) as mongo_client:
        database_ = mongo_client.get_database(database)
        database_[collection].insert_many(deepcopy(records))

    # divides evenly total nrows, 10/5 = 2
    b = read_mongo(
        database,
        collection,
        chunksize=5,
        connection_kwargs=connection_kwargs,
    )

    assert b.npartitions == 2
    assert tuple(b.map_partitions(lambda rec: len(rec)).compute()) == (5, 5)

    # does not divides evenly total nrows, 10/3 -> 4
    b = read_mongo(
        database,
        collection,
        chunksize=3,
        connection_kwargs=connection_kwargs,
    )

    assert b.npartitions == 4
    assert tuple(b.map_partitions(lambda rec: len(rec)).compute()) == (
        3,
        3,
        3,
        1,
    )
