from kafka import KafkaConsumer
import json
import pymongo

def write_data(sub,msg):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["reddit"]
    mycol=mydb[sub]
    x = mycol.insert_one(msg)

def consumer():
    print("Consumer started...")
    consumer= KafkaConsumer('test',bootstrap_servers=['localhost:9092'],api_version=(0,10))
    for message in consumer:
        msg=json.loads(message.value)
        sub=msg['sub']        
        msg.pop('sub')
        write_data(sub,msg)
    
if __name__=="__main__":
    consumer()