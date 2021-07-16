# Dask-Mongo

[![Tests](https://github.com/coiled/dask-mongo/actions/workflows/tests.yml/badge.svg)](https://github.com/coiled/dask-mongo/actions/workflows/tests.yml) [![Linting](https://github.com/coiled/dask-mongo/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/coiled/dask-mongo/actions/workflows/pre-commit.yml)

Read and write data to MongoDB with Dask

## Installation 

You can install `dask-mongo` from PyPI 

```
pip install dask-mongo
```

## Example

```python
import dask.bag as db
import dask_mongo

# Create Dask Bag
records = [
    {"name": "Alice", "fruit": "apricots"},
    {"name": "Bob", "fruit": ["apricots", "cherries"]},
    {"name": "John", "age": 17, "sports": "cycling"},
]

b = db.from_sequence(records)

# Write to a Mongo database
dask_mongo.to_mongo(
    b,
    database="your_database",
    collection="your_collection",
    connection_kwargs={"host": "localhost", "port": 27017},
)

# Read Dask Bag from Mongo database
b = dask_mongo.read_mongo(
    database="your_database",
    collection="your_collection",
    connection_kwargs={"host": "localhost", "port": 27017},
    chunksize=2,
)

# Perform normal operations with Dask
names = b.pluck("name").compute()
assert names == ["Alice", "Bob", "John"]
```

## License 

[BSD-3](LICENSE)
