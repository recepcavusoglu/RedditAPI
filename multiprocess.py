import praw
import json
import concurrent.futures
from kafka import KafkaProducer
#import datetime

#get new data with timer
#add redis
#check latest data

def get_subs(path="config/subreddits.json"):    
    with open(path) as f:
        subs= json.load(f)
    return subs['subreddits']

def get_user_subs():
    reddit = create_reddit_object()
    subs=list(reddit.user.subreddits(limit=None))
    for i in range(len(subs)):
        subs[i]=subs[i].display_name
    return subs

def create_reddit_object(path="config/reddit_config.json"):
    with open(path) as f:
        user_values= json.load(f)
    reddit=praw.Reddit(client_id=user_values['client_id'],
                   client_secret=user_values['client_secret'],
                   user_agent=user_values['user_agent'],
                   username=user_values['username'],password=user_values['password'])
    return reddit


def producer(p_message,p_topicname):
    try:
        producer= KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,10,1)) #add value_serializer parameter 
        message=json.dumps(p_message,ensure_ascii=False).encode('utf-8')
        producer.send(p_topicname,message)
    except Exception as e:
        print("Producer Error: ",e)

def get_sub_data(sub,post_count=100):
    try:
        reddit= create_reddit_object()
        print(f"{sub} data Collecting...")
        subred=reddit.subreddit(sub)
        new= subred.new(limit=post_count)
    except Exception as e:
        print("API Error: ",e)
        return None

    for i in new:
        producer({"sub":sub,"title":i.title,"author":str(i.author),"shortlink":i.shortlink},'test')


if __name__=="__main__":
    with concurrent.futures.ProcessPoolExecutor() as executor:
        subreds=get_user_subs()    
        results=executor.map(get_sub_data,subreds)
    