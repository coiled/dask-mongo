import random
import subprocess
from copy import deepcopy

import dask.bag as db
import pymongo
import pytest
from dask.bag.utils import assert_eq
from distributed.utils_test import cluster_fixture  # noqa: F401
from distributed.utils_test import gen_cluster  # noqa: F401
from distributed.utils_test import cleanup, client, loop, loop_in_thread  # noqa: F401
from pymongo.encryption_options import AutoEncryptionOpts

from dask_mongo import read_mongo, to_mongo
from dask_mongo.core import (
    _CACHE_SIZE,
    _CLIENTS,
    FrozenKwargs,
    _cache_inner,
    _close_clients,
    _get_client,
)


def _get_num_clients():
    return _cache_inner.cache_info().currsize


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


def test_connection_pooling(connection_kwargs):
    records = gen_data(size=10)
    database = "test-db"
    collection = "test-collection"
    _close_clients()
    _cache_inner.cache_clear()
    mongo_client = _get_client(connection_kwargs)
    database_ = mongo_client.get_database(database)
    database_[collection].insert_many(deepcopy(records))
    for _ in range(3):
        read_mongo(
            database,
            collection,
            chunksize=5,
            connection_kwargs=connection_kwargs,
        )
    assert _get_num_clients() == 1
    connection_kwargs.update({"host": ["localhost", "localhost:27017"]})
    read_mongo(
        database,
        collection,
        chunksize=5,
        connection_kwargs=connection_kwargs,
    )

    assert _get_num_clients() == 2
    for i in range(round(_CACHE_SIZE * 1.2)):
        connection_kwargs.update({"maxPoolSize": i})
        read_mongo(
            database,
            collection,
            chunksize=5,
            connection_kwargs=connection_kwargs,
        )
    _close_clients()

    assert _get_num_clients() == _CACHE_SIZE
    assert len(_CLIENTS) == 0


def test_connection_pooling_hashing(connection_kwargs):
    _close_clients()
    _cache_inner.cache_clear()

    opts1 = AutoEncryptionOpts(
        {"local": {"key": b"\x00" * 96}},
        "k.d",
        key_vault_client=_get_client(connection_kwargs),
    )
    client1 = _get_client(dict(connection_kwargs, **{"auto_encryption_opts": opts1}))
    client2 = _get_client(
        dict(
            connection_kwargs,
            **{"auto_encryption_opts": opts1, "host": ["localhost", "localhost:27017"]},
        )
    )
    opts3 = AutoEncryptionOpts(
        {"local": {"key": b"\x00" * 96}},
        "k.d",
        key_vault_client=_get_client(connection_kwargs),
    )
    client3 = _get_client(dict(connection_kwargs, **{"auto_encryption_opts": opts3}))

    def get_client_opts(client):
        return client._MongoClient__options._ClientOptions__auto_encryption_opts

    assert get_client_opts(client1) == get_client_opts(client2)
    assert get_client_opts(client1) != get_client_opts(client3)
    assert get_client_opts(client2) != get_client_opts(client3)
    for opts, c in [(opts1, client1), (opts1, client2), (opts3, client3)]:
        assert opts == get_client_opts(c)

    a = FrozenKwargs({"auto_encryption_opts": opts3})
    a_hash = hash(a)
    a["foo"] = ["bar"]
    assert a_hash != hash(a)
