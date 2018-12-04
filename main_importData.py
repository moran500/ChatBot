import import_mongoDb.ImportMongoDb as mongo
import json
from datetime import datetime

# the month which we would like to import
timeframe = '2015-05'


if __name__ == '__main__':
    
    # count of rows in imported file
    row_counter = 0
    # count of pairs (question and answer) from the imported file, these pairs will be used for training data
    paired_rows = 0
    
    # create Class instance
    db = mongo.ImportMongoDb()
    # create collection 'parent_reply' if not exist
    db.create_collection('parent_reply')
    
    # open the file which will be imported and iterate through the file row by row
    # function format is replacing {} in string with the variables in arguments, you can have from 1 to x arguments
    with open('E:\learningData\ChatBot\RC_{}'.format(timeframe), buffering=1000) as f:
        for row in f:
            row_counter += 1
            # load row in JSON format
            row = json.loads(row)
            parent_id = row['parent_id']
            body = db.format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            comment_id = row['name']
            subreddit = row['subreddit']
            
            # get the parent comment based on parent_id
            parent_data = db.find_parent(parent_id)
            
            # insert or update for comments with score more then 2
            if score >= 2:
                existing_comment_score = db.find_existing_score(parent_id)
                if existing_comment_score:
                    if score > existing_comment_score:
                        if db.acceptable(body):
                            # update of the comment
                            db.sql_insert_replace_comment(comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
                else:
                    if db.acceptable(body):
                        if parent_data:
                            # insert of the comment with pair (parent)
                            db.sql_insert_has_parent(comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
                            paired_rows += 1
                        else:
                            # insert of the new comment
                            db.sql_insert_no_parent(comment_id,parent_id,body,subreddit,created_utc,score)
            
            if row_counter % 100000 == 0:
                print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_counter, paired_rows, str(datetime.now())))
                            
                            
                            
                            
                            