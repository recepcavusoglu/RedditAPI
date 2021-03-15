from kafka import KafkaConsumer
import json
import pymongo

def get_config(path="config/mongo_config.json"):
    with open(path) as f:
        config= json.load(f)
    return config["client_address"],config["database_name"]

def connect_database():
    address,name=get_config()
    myclient = pymongo.MongoClient(address)
    database= myclient[name]
    return database

def create_table(tablename,msg):
    database= connect_database()
    mycol=database[tablename]
    x=mycol.insert_one(msg)
    print(tablename," crated")

def write_data(sub,msg):
    database= connect_database()
    colnames=database.list_collection_names()
    if sub in colnames:
        mycol=database[sub]
        x = mycol.insert_one(msg)
        print(sub," inserted")
    else:
        create_table(sub,msg)

def consumer():
    print("Consumer started...")
    consumer= KafkaConsumer('test',bootstrap_servers=['localhost:9092'],api_version=(0,10))
    for message in consumer:
        msg=json.loads(message.value)
        print(msg)
        sub=msg['sub']        
        msg.pop('sub')
        write_data(sub,msg)
    
if __name__=="__main__":
    consumer()