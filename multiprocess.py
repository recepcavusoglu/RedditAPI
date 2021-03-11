import praw
import json
import concurrent.futures
import time

def create_reddit_object(json_file="reddit_config.json"):
    with open(json_file) as f:
        user_values= json.load(f)
    reddit=praw.Reddit(client_id=user_values['client_id'],
                   client_secret=user_values['client_secret'],
                   user_agent=user_values['user_agent'],
                   username=user_values['username'],password=user_values['password'])
    return reddit



def getData(sub,count=100):
    reddit= create_reddit_object()
    print(f"{sub} data Collecting...")
    subred=reddit.subreddit(sub)
    new= subred.new(limit=count)
    data=[]
    for i in new:
        data.append({"title":i.title,"author":str(i.author),"shortlink":i.shortlink})
    return data


if __name__=="__main__": 
    start = time.perf_counter()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        subreds=["turkey","ankara","learnprogramming"]
        results=executor.map(getData,subreds)
        data=[]
        for result in results:
            data.append(result)
    print("stop")
    #Burada dump yapmak yerine kafkaya yolla
    for i in range(len(subreds)):
        filename=subreds[i]+'.json'
        with open(filename, 'w') as fout:
            json.dump(data[i] , fout)
    finish=time.perf_counter()
    print("Finished: ", (finish-start))
    x=input("Press Enter to Exit")