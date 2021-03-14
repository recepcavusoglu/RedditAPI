from kafka import KafkaConsumer
import json
import pymongo

def connect_database():#get connection from config file
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    return myclient

def create_table(tablename):#table create
    pass

def write_data(sub,msg):
    myclient= connect_database()
    database = myclient["reddit"]
    mycol=database[sub]
    x = mycol.insert_one(msg)
    print("instered")
    myclient.close()

def consumer():
    print("Consumer started...")
    consumer= KafkaConsumer('test',bootstrap_servers=['localhost:9092'],api_version=(0,10))
    i=0
    for message in consumer:
        msg=json.loads(message.value)
        print(i)
        i+=1
        sub=msg['sub']        
        msg.pop('sub')
        write_data(sub,msg)
    
if __name__=="__main__":
    consumer()