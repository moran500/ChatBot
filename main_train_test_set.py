import import_mongoDb.ImportMongoDb as mongo
import pandas as pd

# this is list of months out of which we would like to create test and train data, for now we have just one month
timeframes = ['2015-05']

if __name__ == '__main__':
    
    # create connection to our MondoDb and open the collection
    db = mongo.ImportMongoDb()
    db.create_collection('parent_reply')
    
    # this for statement iterates over list of months which we have in timeframes list
    for timeframe in timeframes:
        
        # this will set the limit for test data set
        limit = 1000
        
        last_unix = 0
        cur_length = limit
        counter = 0
        test_done = False
        
        while cur_length == limit:
            
            # get data from MongoDb
            cursor = db.sql_read_from_mongoDb(last_unix, limit)
            # store the data from MongoDb in pandas dataframe
            df = pd.DataFrame(list(cursor))
            # this will get the last row from data frame and store the unix value in last_unix
            last_unix = df.tail(1)['unix_int'].values[0]
            cur_length = len(df)
            
            if not test_done:
                # this is opening file and write there the test data set 
                with open('test.from', 'a', encoding='utf8') as f:
                    for content in df['parent'].values:
                        f.write(str(content) + '\n')
                
                # this is opening file and write there the test data set        
                with open('test.to', 'a', encoding='utf8') as f:
                    for content in df['comment'].values:
                        f.write(str(content) + '\n')
                        
                test_done = True
            
            else:
                # this is opening file and write there the train data set
                with open('train.from', 'a', encoding='utf8') as f:
                    for content in df['parent'].values:
                        f.write(str(content) + '\n')
                
                # this is opening file and write there the train data set        
                with open('train.to', 'a', encoding='utf8') as f:
                    for content in df['comment'].values:
                        f.write(str(content) + '\n')
            
            counter += 1
            
            if counter % 5 == 0:
                print(counter*limit,'rows completed so far')   
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        