import tweepy
import json
from kafka import KafkaProducer
import time
import sys

ckey = 'VpaEFU28gqdj2hWF6HWX4T7vm'
csecret = 'TFx9R6LSpJQAIgeypflR7osTbRqPuiYw8PM98ocT1NprL6IHTA'
atoken = '4167206710-o67meh2UwYNrKxKeJebeUsnDXJSkKZZQTCVIkE1'
asecret = 'NUeYqPt2DJCZFBj43naelDF650EKe6EfX9uddgfOtJGXO'

topic_users = "twitter_users"
topic_followers = "twitter_followers"
producer = KafkaProducer(bootstrap_servers='ec2-52-3-154-136.compute-1.amazonaws.com:9092')

class TweetStreamProducer(tweepy.StreamListener):

  def on_data(self, data):
    json_data = json.loads(data)
    screen_name = str(json_data['user']['screen_name'])
    print json_data['created_at'], "user sent"
    producer.send(topic_users,"user,"+str(json_data['id'])+","+str(json_data['created_at'])+","+str(json_data['text'].encode('utf-8'))+","+str(json_data['user']['screen_name'])+","+str(json_data['user']['followers_count'])+'\n')
    while True:
      try:
        user = api.get_user(screen_name)

        for follower in user.followers()[:10]:
          follower_screen_name = follower.screen_name
          print json_data['created_at'], "follower sent"
          producer.send(topic_users,"follower,"+str(json_data['id'])+","+str(json_data['created_at'])+str(json_data['text'].encode('utf-8'))+","+str(follower_screen_name)+","+str(json_data['user']['followers_count'])+'\n')
          producer.send(topic_followers,str(screen_name)+','+str(follower_screen_name)+'\n')
        break;
      except tweepy.TweepError,e:
        print e
        time.sleep(60*15)
        pass  
      except StopIteration:
        break
      except Exception,e:
        print str(sys.exc_info()[0])
        time.sleep(1)
        pass


  def on_error(self, status_code):
    print >> sys.stderr, 'Encountered error with status code:', status_code
    return True # Don't kill the stream

  def on_timeout(self):
    print >> sys.stderr, 'Timeout...'
    return True # Don't kill the stream


def start_stream():
    while True:
        try:
          twitterStream = tweepy.streaming.Stream(auth, TweetStreamProducer())
          twitterStream.sample(languages=["en"])
        except: 
            continue

def start():
  try:
    global api
    auth = tweepy.OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    api = tweepy.API(auth)
    twitterStream = tweepy.streaming.Stream(auth, TweetStreamProducer())
    twitterStream.filter(languages=["en"],track=["#Knicks"])
  except Exception,e:
    print "error: str(e)"


if __name__ == "__main__":
  start()