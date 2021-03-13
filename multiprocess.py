import praw
import json
import concurrent.futures
from kafka import KafkaProducer

def create_reddit_object(json_file="reddit_config.json"):
    with open(json_file) as f:
        user_values= json.load(f)
    reddit=praw.Reddit(client_id=user_values['client_id'],
                   client_secret=user_values['client_secret'],
                   user_agent=user_values['user_agent'],
                   username=user_values['username'],password=user_values['password'])
    return reddit


def Producer(p_message,p_topicname):
    producer= KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,10,1)) #add value_serializer parameter 
    message=json.dumps(p_message,ensure_ascii=False).encode('utf-8')
    producer.send(p_topicname,message)

def getData(sub,count=50):
    reddit= create_reddit_object()
    print(f"{sub} data Collecting...")
    subred=reddit.subreddit(sub)
    new= subred.new(limit=count)    
    for i in new:
        Producer({"sub":sub,"title":i.title,"author":str(i.author),"shortlink":i.shortlink},'test')


if __name__=="__main__":
    with concurrent.futures.ProcessPoolExecutor() as executor:
        subreds=["turkey","learnprogramming","ankara"]
        results=executor.map(getData,subreds)