from kafka import KafkaConsumer
import json
import pandas as pd
from databaseoperations import collection,db

def alreadyExists(newID):
    i =0
    for each in db.collection.find({'last_updated': last_updated}):
        i += 1
    if i == 0:
        return False
    else:
        return True


if __name__ == "__main__" :
    consumer = KafkaConsumer(
        "NewYorkTimes",
        bootstrap_servers = 'localhost:9092',
        auto_offset_reset='earliest',
        group_id="consumer-group-a")
    print("starting the consumer")
    
    for msg in consumer:
        record = json.loads(msg.value)
        
        status = record['status']
        copyright = record['copyright']
        section = record['section']
        last_updated = record['last_updated']
        num_results = record['num_results']
        results = record['results']
        
        topstories = {'last_updated':last_updated, 'results':results}

        if alreadyExists(last_updated): 
            pass
        else:
            rec_id1 = db.collection.insert_one(topstories)
            print("Data inserted with record ids", rec_id1) 


