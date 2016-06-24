import tweepy
import json
from kafka import KafkaProducer
import avro.schema
import io, random
from avro.io import DatumWriter

ckey = 'VpaEFU28gqdj2hWF6HWX4T7vm'
csecret = 'TFx9R6LSpJQAIgeypflR7osTbRqPuiYw8PM98ocT1NprL6IHTA'
atoken = '4167206710-o67meh2UwYNrKxKeJebeUsnDXJSkKZZQTCVIkE1'
asecret = 'NUeYqPt2DJCZFBj43naelDF650EKe6EfX9uddgfOtJGXO'

topic = "twitter_test3"
producer = KafkaProducer(bootstrap_servers='ec2-52-203-103-83.compute-1.amazonaws.com:9092')
# Path to user.avsc avro schema
schema_path="tweet.avsc"
schema = avro.schema.parse(open(schema_path).read())


class TweetStreamProducer(tweepy.StreamListener):

   def on_data(self, data):
   		json_data = json.loads(data)
	   	writer = avro.io.DatumWriter(schema)
	   	bytes_writer = io.BytesIO()
	   	encoder = avro.io.BinaryEncoder(bytes_writer)
	   	writer.write({"tweet_id": str(json_data['id']), "screen_name": str(json_data['user']['screen_name']), "followers_count": str(json_data['user']['followers_count'])}, encoder)
	   	raw_bytes = bytes_writer.getvalue()
	   	print str(json_data['id']), " sent"
	   	producer.send(topic,raw_bytes)
	   	return True

   def on_error(self, status_code):
       print >> sys.stderr, 'Encountered error with status code:', status_code
       return True # Don't kill the stream

   def on_timeout(self):
       print >> sys.stderr, 'Timeout...'
       return True # Don't kill the stream

def start():
   auth = tweepy.OAuthHandler(ckey, csecret)
   auth.set_access_token(atoken, asecret)
   twitterStream = tweepy.streaming.Stream(auth, TweetStreamProducer())
   twitterStream.sample(languages=["en"])

if __name__ == "__main__":
   start()

