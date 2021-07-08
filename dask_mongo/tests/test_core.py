from pymongo_inmemory import MongoClient


def test_no_database():

    client = MongoClient()

    assert client.list_database_names() == ["admin", "config", "local"]

    # check that until no empty collection is added new database doesn't exist
    client["new_db"]
    assert "new_db" not in client.list_database_names()
    assert not client["new_db"].list_collection_names()

    client["new_db"]["new_coll"]
    assert "new_db" not in client.list_database_names()
    assert not client["new_db"].list_collection_names()

    client["new_db"]["new_collection"].insert_one({"x": 1})
    assert "new_db" in client.list_database_names()
    assert client["new_db"].list_collection_names() == ["new_collection"]

    client["new_db"].drop_collection("new_collection")
    assert "new_db" not in client.list_database_names()

    client.close()
