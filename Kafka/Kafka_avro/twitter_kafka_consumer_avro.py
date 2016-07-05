from kafka import KafkaConsumer
import avro.schema
import avro.io
import io
 
# To consume messages
consumer = KafkaConsumer('twitter_test3',bootstrap_servers='ec2-52-203-103-83.compute-1.amazonaws.com:9092')
 
schema_path="tweet.avsc"
schema = avro.schema.parse(open(schema_path).read())
 
for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    tweet = reader.read(decoder)
    print tweet