import pymongo

class ImportMongoDb():

    # connect to MongoDb
    mng_client = pymongo.MongoClient('localhost', 27017)
    # create db if doesnt exist    
    mng_db = mng_client['ChatBot_Reddit']
    collection_name = ''
    

    def create_collection(self, pCollectionName):
        # create collection in mongoDb if not exist
        self.mng_db[pCollectionName]
        self.collection_name = pCollectionName

    def format_data(self,pBody):
        # replace new line this fuction is not fitting to this class but Im lazy to create new Class in new package for this function and I do not want this function in main file
        data = pBody.replace('\n',' newlinechar ').replace('\r',' newlinechar ').replace('"',"'")
        return data

    def find_parent(self, pParentId):
        try:
            result = self.mng_db[self.collection_name].find({"comment_id" : pParentId}).limit(1)
            if result.count() > 0:
                for record in result:
                    return record['comment']
            else:
                return False
        except Exception as e:
            print(e)
            return False   
        
    def find_existing_score(self, pParentId):
        try:
            result = self.mng_db[self.collection_name].find({"parent_id" : pParentId}).limit(1)
            if result.count() > 0:
                for record in result:
                    return record['score']
            else:
                return False
        except Exception as e:
            print(e)
            return False
        
    def acceptable(self, pData):
        if len(pData.split(' ')) > 50 or len(pData) < 1:
            return False
        elif len(pData) > 1000:
            return False
        elif pData == '[deleted]':
            return False
        elif pData == '[removed]':
            return False
        else:
            return True
        
    def sql_insert_replace_comment(self, pCommentId, pParentId, pParentData, pBody, pSubreddit, pCreated_utc, pScore):
        try:
            # this is update of existing documents in mongoDb based on the condition
            condition = { "parent_id": pParentId }
            new_value = { "$set": {"parent_id" : pParentId, "comment_id" : pCommentId, "parent" : pParentData, "comment" : pBody, "subreddit" : pSubreddit, "unix" : pCreated_utc, "score" : pScore} }
            self.mng_db[self.collection_name].update_many(condition, new_value)
        except Exception as e:
#             print('s0 insertion',str(e))
            raise e
        
    def sql_insert_has_parent(self, pCommentId, pParentId, pParentData, pBody, pSubreddit, pCreated_utc, pScore):
        try:
            # this is insert of new document to mongoDb
            new_document = {"parent_id": pParentId, "comment_id": pCommentId, "parent": pParentData, "comment": pBody, "subreddit": pSubreddit, "unix": pCreated_utc, "score": pScore}
            self.mng_db[self.collection_name].insert_one(new_document)
        except Exception as e:
#             print('s0 insertion',str(e))
            raise e
    
    def sql_insert_no_parent(self, pCommentId, pParentId, pBody, pSubreddit, pCreated_utc, pScore):
        try:
            # this is insert of new document to mongoDb
            new_document = {"parent_id": pParentId, "comment_id": pCommentId, "comment": pBody, "subreddit": pSubreddit, "unix": pCreated_utc, "score": pScore}
            self.mng_db[self.collection_name].insert_one(new_document)
        except Exception as e:
#             print('s0 insertion',str(e))
            raise e
    
    def sql_read_from_mongoDb(self, lastUnit, limit):
        # this is getting data from MongoDb with special conditions and limit of records and also sorted ascending  
        try:
            return self.mng_db[self.collection_name].find({"$and": [{"score": {"$gt": 0}}, {"unix_int": {"$gt": lastUnit}}, {"parent":{"$ne": None}}]}).limit(limit).sort([("unix_int", 1)])
        except Exception as e:
            raise e
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 