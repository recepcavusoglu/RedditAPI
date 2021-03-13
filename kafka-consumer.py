from kafka import KafkaConsumer
import json
import pymongo

consumer= KafkaConsumer('test',bootstrap_servers=['localhost:9092'],api_version=(0,10))
print("Consumer started...")

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["reddit"]


for message in consumer:
    msg=json.loads(message.value)
    sub=msg['sub']
    mycol=mydb[sub]
    msg.pop('sub')
    x = mycol.insert_one(msg)