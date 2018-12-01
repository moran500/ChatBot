import pandas as pd
import pymongo
import json

mng_client = pymongo.MongoClient('localhost', 27017)
mng_db = mng_client['stocks'] 
collection_name = 'SP500'
db_cm = mng_db[collection_name]
data = pd.read_csv('C:\PrivateData\Learning\MongoDB_udemy\data\SP500.csv')
data_json = json.loads(data.to_json(orient='records'))
db_cm.insert(data_json)

