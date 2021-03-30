# RedditAPI
> Regularly getting subreddit data and storing it using different technologies.

**Used Technologies**:
* Redis: Stores last pulled posts unixtime to prevent data duplication each subreddit.
* Kafka: Producer get data and send it to consumer who stores data to MongoDB.
* MongoDB: Stores data.

<p align="center">
  <img src="/images/system.png">
</p>

## Config Files
> reddit_config.json: Stores api configs
```json
{
    "client_id":"personal use script key code 14 chars",
    "client_secret":"28 chars secret",
    "user_agent":"APP NAME",
    "username":"reddit user name",
    "password":"reddit user password"
}
```
> mongo_config.json: Stores MongoDB configs
```json
{
    "client_address":"Client address",
    "database_name":"Database Name"
}
```
> redis_config.json: Stores Redis configs
```json
{
    "host":"Host Address",
    "port":"Port Number(This value must be int)"
}
```
> kafka_config.json: Stores Kafka configs
```json
{
    "host":"Host Address",
    "topic_name":"Topic Name"
}
```
> subreddits.json: Stores subreddits list

```json
{
    "subreddits":["subreddit1","subreddit2"]    
}
```

## Usage

> Getting subreddit data via json
```cmd
python multiprocess.py json
```
> Getting subreddit data via user follow
```cmd
python multiprocess.py follow
```
