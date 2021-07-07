# Dask-Mongo

This connector is an early experiment to read from/ write to a mongo database 

```python
import dask
import dask_mongo

# Create Dask DataFrame with random data
df = dask.datasets.timeseries()

# Write DataFrame to Mongo database
dask_mongo.to_mongo(df, ...)

# Read DataFrame from Mongo database
df = dask_mongo.read_mongo(...)

# Perform normal operations with Dask
df.x.mean().compute()
```