import tweepy
import json
import time
import sys
import yaml
from kafka import KafkaProducer

with open("producerconfig.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
#print cfg['kafka']['broker_list']
producer = KafkaProducer(bootstrap_servers=cfg['kafka']['broker_list'])


class TweetStreamProducer(tweepy.StreamListener):

  def on_data(self, data):
    if 'limit' in data or 'delete' in data or 'warning' in data:
      return 
    json_data = json.loads(data)
    if json_data['lang'] == "en":
      tweet_info = str(json_data['id'])+","+str(json_data['created_at'])+","+str(json_data['user']['screen_name'])+","+str(json_data['user']['followers_count'])
      producer.send(cfg['kafka']['topic']['users'],tweet_info+"\n")
      #print json_data['created_at'], "user sent"

  def on_error(self, status_code):
    print >> sys.stderr, 'Encountered error with status code:', status_code
    return True # Don't kill the stream

  def on_timeout(self):
    print >> sys.stderr, 'Timeout...'
    return True # Don't kill the stream

def start_user_stream():
  while True:
    try:
      user_auth = tweepy.OAuthHandler(cfg['twitter']['user']['ckey'], cfg['twitter']['user']['csecret'])
      user_auth.set_access_token(cfg['twitter']['user']['atoken'], cfg['twitter']['user']['asecret'])
      twitterStream = tweepy.streaming.Stream(user_auth, TweetStreamProducer())
      twitterStream.filter(languages=["en"],track=cfg['twitter']['keywords'])
    except: 
      continue


if __name__ == "__main__":
  start_user_stream()
