import argparse
import praw
import json
import concurrent.futures
from kafka import KafkaProducer
import schedule
import sys
import redis
#import datetime

#sen data and check from redis

def create_redis(path="config/redis_config.json"):
    with open(path) as f:
        config=json.load(f)
    client=redis.Redis(host=config['host'],port=config['port'])
    return client


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

def get_sub_data(sub,post_count=1500):
    try:
        reddit= create_reddit_object()
        print(f"{sub} data Collecting...")
        #pull redis data
        #if redisdata empty:
        new=reddit.subreddit(sub).new(limit=post_count)
        #else:
        #new=reddit.subreddit(sub).new(unixtimestamp start-finish period)
    except Exception as e:
        print("API Error: ",e)
        return None

    for i in new:
        producer({"sub":sub,"title":i.title,"author":str(i.author),"shortlink":i.shortlink},'test')

def call_data(p_sublist):
    if p_sublist:
        subreds=get_subs()
    else:
        subreds=get_user_subs()

    with concurrent.futures.ProcessPoolExecutor() as executor:          
        results=executor.map(get_sub_data,subreds)

def arg_parser():
    parser=argparse.ArgumentParser()
    parser.add_argument("sublist",help="Please specify subreddit list using either json or follow")
    args=parser.parse_args()
    if args.sublist=="json":
        print("Getting subreddit data via json file...")
        return True
    elif args.sublist=="follow":
        print("Getting subreddit data via users follow...")
        return False
    else:
        print("Wrong usage plese use either [json] or [follow] paramater.")
        sys.exit(0)

if __name__=="__main__":
    sublist=False
    #sublist=arg_parser()
    call_data(sublist)
    schedule.every(1).minutes.do(call_data,sublist)
    while True:
        schedule.run_pending()

    