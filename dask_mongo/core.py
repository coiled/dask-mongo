import pymongo
from pymongo import MongoClient
import json 

import dask
import dask.dataframe as dd

#write to mongo db 



def check_db_exists(client, db):
    db_names = client.list_database_names()

    try:
        db in db_names
    except ValueError:
        print(f"The database {db} doesn not exists")
        return 


#standard connection string format to connect 
# https://docs.mongodb.com/manual/reference/connection-string/
#If hosts is URI: 
# mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[defaultauthdb][?options]]

def to_mongo(
    df,
    host, #hostname or IP address of the instance to connect to, or a mongodb URI, or a list of hostnames / mongodb URIs. this is optional how do we say that
    database: str, #name of data base
):

    #1. convert a dask dataframe into dicts
    #2. use insert_many to dump the dicst into DB

    mongo_client = MongoClient(host)

    #we should check that the database exists probably
    check_db_exists(mongo_client, database)

    #get database
    db = mongo_client.get_database(database)

    #convert df to dict -> do we convert to json and then dicst? 

    #how do we create the to_json using dask
    #docs say 
    # Location to write to. If a string, and there are more than one partitions in df, 
    # should include a glob character to expand into a set of file names, or provide a name_function= parameter.

    dask.dataframe.to_json(df, url_path='df.json')  

    with open('df.json') as f:
        file_data = json.load(f)
    
    db.insert_many(file_data)

    mongo_client.close()
    