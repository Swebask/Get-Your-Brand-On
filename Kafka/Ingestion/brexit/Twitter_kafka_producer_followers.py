import tweepy
import json
import time
import sys
import yaml
from kafka import KafkaProducer
from kafka import KafkaConsumer


with open("producerconfig.yml", 'r') as ymlfileProd:
    cfgProd = yaml.load(ymlfileProd)
producer = KafkaProducer(bootstrap_servers=cfgProd['kafka']['broker_list'])

with open("consumerconfig.yml", 'r') as ymlfileCons:
    cfgCons = yaml.load(ymlfileCons)

follower_auth = tweepy.OAuthHandler(cfgProd['twitter']['follower']['ckey'], cfgProd['twitter']['follower']['csecret'])
follower_auth.set_access_token(cfgProd['twitter']['follower']['atoken'], cfgProd['twitter']['follower']['asecret'])
api = tweepy.API(follower_auth)

class Consumer(object):
    def __init__(self, topic, bootstrapServers):
        #Initialize Consumer with kafka broker IP, and topic.
        self.topic = topic
        self.consumer = KafkaConsumer(topic, bootstrap_servers=bootstrapServers)

    def consume_topic(self):
        while True:
            try:
                for message in self.consumer:
                    start_follower_stream(str(message.value))
            except:
                print ("error")

def start_follower_stream(tweet_info):
  while True:
    try:
      #print tweet_info.split(',')[2], "user received"
      user = api.get_user(tweet_info.split(',')[2])
      for follower in user.followers()[:10]:
        follower_screen_name = follower.screen_name
        producer.send(cfgProd['kafka']['topic']['followers'],tweet_info+","+str(follower_screen_name)+'\n')
        #print follower_screen_name, "follower sent"
      return
    except tweepy.TweepError,e:
      print e
      time.sleep(60*15)
      pass  
    except Exception,e:
      print e
      time.sleep(10)
      pass

if __name__ == "__main__":
  cons = Consumer(cfgCons['kafka']['topic']['users'],cfgCons['kafka']['broker_list'])
  cons.consume_topic()
