import tweepy
import json
from kafka import KafkaProducer
import time

ckey = 'VpaEFU28gqdj2hWF6HWX4T7vm'
csecret = 'TFx9R6LSpJQAIgeypflR7osTbRqPuiYw8PM98ocT1NprL6IHTA'
atoken = '4167206710-o67meh2UwYNrKxKeJebeUsnDXJSkKZZQTCVIkE1'
asecret = 'NUeYqPt2DJCZFBj43naelDF650EKe6EfX9uddgfOtJGXO'

topic = "twitter_test"
producer = KafkaProducer(bootstrap_servers='ec2-52-203-103-83.compute-1.amazonaws.com:9092')

class TweetStreamProducer(tweepy.StreamListener):

  def on_data(self, data):
    json_data = json.loads(data)
    screen_name = str(json_data['user']['screen_name'])
    follower_list = ''
    pages = tweepy.Cursor(api.followers_ids, screen_name=screen_name).items()
    while True:
      try:
        users = []
        for page in pages:
          users.extend(page)
          print users
        #   print users
        #   for user in users:
        #     follower_list = follower_list + "," + user.screen_name
      except tweepy.TweepError:
        time.sleep(60 * 15)
        continue
      except StopIteration:
        break
    print follower_list 
    producer.send(topic,str(json_data['id'])+","+str(json_data['user']['screen_name'])+","+str(json_data['user']['followers_count']+follower_list))
    print str(json_data['id']), " sent"
    return True

  def on_error(self, status_code):
    print >> sys.stderr, 'Encountered error with status code:', status_code
    return True # Don't kill the stream

  def on_timeout(self):
    print >> sys.stderr, 'Timeout...'
    return True # Don't kill the stream

def start():
  
   global api
   auth = tweepy.OAuthHandler(ckey, csecret)
   auth.set_access_token(atoken, asecret)
   api = tweepy.API(auth)
   twitterStream = tweepy.streaming.Stream(auth, TweetStreamProducer())
   twitterStream.sample(languages=["en"])

if __name__ == "__main__":
   start()