import pymongo
import pprint

if __name__ == '__main__':
    
    mng_client = pymongo.MongoClient('localhost', 27017)
    # create db if doesnt exist    
    mng_db = mng_client['ChatBot_Reddit']
    
    coll = mng_db['parent_reply']


#     result = coll.find().limit(10)
    
    lastUnit = 0
    limit = 1000
    
#     result = coll.find({"$and": [{"score": {"$gt": 0}}, {"unix_int": {"$gt": 0}}]}).limit(1000).sort({"unix_int": 1})
    
    result = coll.find({"$and": [{"score": {"$gt": 0}}, {"unix_int": {"$gt": 0}}]}).limit(1000).sort([("unix_int", 1)])


    for record in result:
        pprint.pprint(record)
        break
    