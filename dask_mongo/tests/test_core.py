import random
import subprocess

import dask.bag as db
import pymongo
import pytest
from dask.bag.utils import assert_eq
from distributed import wait
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
async def test_to_mongo(c, s, a, b, connection_args):
    records = gen_data(size=10)
    npartitions = 3
    b = db.from_sequence(records, npartitions=npartitions)

    with pymongo.MongoClient(**connection_args) as mongo_client:
        database = "test-db"
        assert database not in mongo_client.list_database_names()
        collection_name = "test-collection"

        partitions = to_mongo(
            b,
            connection_args=connection_args,
            database=database,
            collection=collection_name,
        )
        assert len(partitions) == npartitions
        await wait(partitions)

        assert database in mongo_client.list_database_names()
        assert [collection_name] == mongo_client[database].list_collection_names()

        results = list(mongo_client[database][collection_name].find())
        # Drop "_id" and sort by "idx" for comparison
        results = [
            {k: v for k, v in result.items() if k != "_id"} for result in results
        ]
        results = sorted(results, key=lambda x: x["idx"])
        assert_eq(b, results)


def test_to_mongo_single_machine_scheduler(connection_args):
    records = gen_data(size=10)
    npartitions = 3
    b = db.from_sequence(records, npartitions=npartitions)

    with pymongo.MongoClient(**connection_args) as mongo_client:
        database = "test-db"
        assert database not in mongo_client.list_database_names()
        collection_name = "test-collection"

        to_mongo(
            b,
            connection_args=connection_args,
            database=database,
            collection=collection_name,
        )

        assert database in mongo_client.list_database_names()
        assert [collection_name] == mongo_client[database].list_collection_names()

        results = list(mongo_client[database][collection_name].find())
        # Drop "_id" and sort by "idx" for comparison
        results = [
            {k: v for k, v in result.items() if k != "_id"} for result in results
        ]
        results = sorted(results, key=lambda x: x["idx"])

        assert_eq(b, results)


def test_read_mongo(connection_args, client):
    records = gen_data(size=10)
    npartitions = 3
    b = db.from_sequence(records, npartitions=npartitions)

    db_name = "test-db"
    collection_name = "test-collection"

    partitions = to_mongo(
        b,
        connection_args=connection_args,
        database=db_name,
        collection=collection_name,
    )

    wait(partitions)

    rec_read = read_mongo(
        connection_args=connection_args,
        database=db_name,
        collection=collection_name,
        chunksize=5,
    )

    results = [{k: v for k, v in result.items() if k != "_id"} for result in rec_read]
    results = sorted(results, key=lambda x: x["idx"])
    assert_eq(b, results)


def test_mongo_roundtrip_single_machine_scheduler(connection_args):
    records = gen_data(size=10)
    npartitions = 3
    b = db.from_sequence(records, npartitions=npartitions)

    db_name = "test-db"
    collection_name = "test-collection"

    to_mongo(
        b,
        connection_args=connection_args,
        database=db_name,
        collection=collection_name,
    )

    # read whole dataframe, match={} is default
    rec_read = read_mongo(
        connection_args=connection_args,
        database=db_name,
        collection=collection_name,
        chunksize=5,
    )

    results = [{k: v for k, v in result.items() if k != "_id"} for result in rec_read]
    results = sorted(results, key=lambda x: x["idx"])
    assert_eq(b, results)


def test_read_mongo_match(connection_args):
    records = gen_data(size=10)
    npartitions = 3
    b = db.from_sequence(records, npartitions=npartitions)

    db_name = "test-db"
    collection_name = "test-collection"

    to_mongo(
        b,
        connection_args=connection_args,
        database=db_name,
        collection=collection_name,
    )

    rec_read = read_mongo(
        connection_args=connection_args,
        database=db_name,
        collection=collection_name,
        chunksize=5,
        match={"idx": {"$gte": 2, "$lte": 7}},
    )

    results = [{k: v for k, v in result.items() if k != "_id"} for result in rec_read]

    results = sorted(results, key=lambda x: x["idx"])

    b_filtered = b.filter(lambda x: 2 <= x["idx"] <= 7)

    assert_eq(b_filtered, results)


def test_read_mongo_chunksize(connection_args):
    records = gen_data(size=10)
    npartitions = 3
    b = db.from_sequence(records, npartitions=npartitions)

    db_name = "test-db"
    collection_name = "test-collection"

    to_mongo(
        b,
        connection_args=connection_args,
        database=db_name,
        collection=collection_name,
    )

    # divides evenly total nrows, 10/5 = 2
    rec_read = read_mongo(
        connection_args=connection_args,
        database=db_name,
        collection=collection_name,
        chunksize=5,
    )

    assert rec_read.npartitions == 2
    assert tuple(rec_read.map_partitions(lambda rec: len(rec)).compute()) == (5, 5)

    # does not divides evenly total nrows, 10/3 -> 4
    rec_read = read_mongo(
        connection_args=connection_args,
        database=db_name,
        collection=collection_name,
        chunksize=3,
    )

    assert rec_read.npartitions == 4
    assert tuple(rec_read.map_partitions(lambda rec: len(rec)).compute()) == (
        3,
        3,
        3,
        1,
    )
