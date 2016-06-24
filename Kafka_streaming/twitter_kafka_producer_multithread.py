import tweepy
import json
import time
import sys
import threading
import yaml
from threading import Thread
from kafka import KafkaProducer
from Queue import Queue

with open("producerconfig.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
tweetQueue = Queue(maxsize=0)
print cfg['kafka']['broker_list']
producer = KafkaProducer(bootstrap_servers=cfg['kafka']['broker_list'])


class TweetStreamProducer(tweepy.StreamListener):

  def on_data(self, data):
    if 'limit' in data or 'delete' in data or 'warning' in data:
      return 
    json_data = json.loads(data)
    tweet_info = str(json_data['id'])+","+str(json_data['created_at'])+","+str(json_data['text'].encode('utf-8'))+","+str(json_data['user']['screen_name'])+str(json_data['user']['followers_count'])
    producer.send(cfg['kafka']['topic']['users'],tweet_info+"\n")
    tweetQueue.put([json_data['user']['screen_name'],tweet_info])
    tweetQueue.task_done()
    print json_data['created_at'], "user sent"

  def on_error(self, status_code):
    print >> sys.stderr, 'Encountered error with status code:', status_code
    return True # Don't kill the stream

  def on_timeout(self):
    print >> sys.stderr, 'Timeout...'
    return True # Don't kill the stream

def start_user_stream():
  #try:
  user_auth = tweepy.OAuthHandler(cfg['twitter']['user']['ckey'], cfg['twitter']['user']['csecret'])
  user_auth.set_access_token(cfg['twitter']['user']['atoken'], cfg['twitter']['user']['asecret'])
  twitterStream = tweepy.streaming.Stream(user_auth, TweetStreamProducer())
  twitterStream.filter(languages=["en"],track=cfg['twitter']['keywords'])

def start_follower_stream():
  follower_auth = tweepy.OAuthHandler(cfg['twitter']['follower']['ckey'], cfg['twitter']['follower']['csecret'])
  follower_auth.set_access_token(cfg['twitter']['follower']['atoken'], cfg['twitter']['follower']['asecret'])
  api = tweepy.API(follower_auth)
  while True:
    try:
      while True:
        if not tweetQueue.empty():
          screen_name, tweet_info = tweetQueue.get()
          break
      user = api.get_user(screen_name)
      for follower in user.followers()[:10]:
        follower_screen_name = follower.screen_name
        producer.send(cfg['kafka']['topic']['followers'],tweet_info+","+str(follower_screen_name)+'\n')
        print follower_screen_name, "follower sent"
    except tweepy.TweepError,e:
      print e
      time.sleep(60*15)
      pass  
    except Exception,e:
      print e
      time.sleep(10)
      pass

if __name__ == "__main__":
  Thread(target = start_user_stream).start()
  Thread(target = start_follower_stream).start()