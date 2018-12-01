import pymongo
import json


class ImportMongoDb():

    # connect to MongoDb
    mng_client = pymongo.MongoClient('localhost', 27017)
    # create db if doesnt exist    
    mng_db = mng_client['ChatBot_Reddit']
    

    def create_collection(self, pCollectionName):
        self.mng_db[pCollectionName]


 
# collection_name = 'SP500'
# db_cm = mng_db[collection_name]
# data = pd.read_csv('C:\PrivateData\Learning\MongoDB_udemy\data\SP500.csv')
# data_json = json.loads(data.to_json(orient='records'))
# db_cm.insert(data_json)

