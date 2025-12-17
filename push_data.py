
# from pymongo.mongo_client import MongoClient
# from pymongo.server_api import ServerApi

# uri = "mongodb+srv://vedantsagwal_db_userpassword@cluster0.6zh6m0t.mongodb.net/?appName=Cluster0"

# # Create a new client and connect to the server
# client = MongoClient(uri, server_api=ServerApi('1'))

# # Send a ping to confirm a successful connection
# try:
#     client.admin.command('ping')
#     print("Pinged your deployment. You successfully connected to MongoDB!")
# except Exception as e:
#     print(e)


import os
import sys
import json

from dotenv import load_dotenv
load_dotenv()


mongodb_url = os.getenv("MONGODB_URI")

import certifi
ca = certifi.where()


import pandas as pd
import numpy as np
import pymongo
from networksecurity.logging.logger import logging
from networksecurity.exception.exception import NetworkSecurityException


class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    def cv_to_json_converter(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_to_mongo(self, records, database, collection):
        try:
            self.database = database
            self.collection = collection
            self.records = records

            self.mongo_client = pymongo.MongoClient(mongodb_url)
            self.database = self.mongo_client[self.database]
            self.collection = self.mongo_client[self.collection]
            self.collection.insert.insert_many(self.records)
            return len(self.records)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

if __name__ == "__main__":
    FILE_PATH = "Network_Data/phisingData.csv"
    print(FILE_PATH)
    DATABASE="SunRaku"
    COLLECTION = "NetworkData"
    networkDataObj = NetworkDataExtract()
    records = networkDataObj.cv_to_json_converter(FILE_PATH)
    print(records)
    no_of_records = networkDataObj.insert_data_to_mongo(records, DATABASE, COLLECTION)
    print(no_of_records)
