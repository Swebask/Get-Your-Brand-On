from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from dateutil import tz
from datetime import datetime
import yaml
from cassandra.cluster import Cluster

# Config files with spark, cassandra and kafka cluster details
with open("streamconfig.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

# Intitializing cassandra cluster and keyspace
cluster = Cluster(cfg['cassandra']['cluster'])
session = cluster.connect()
session.execute("USE reach;")

# Setting TimeZone to Eastern Time
from_zone = tz.gettz('UTC')
to_zone = tz.gettz('America/New_York')

sc = SparkContext(cfg['spark']['cluster'], "ReachCount")
ssc = StreamingContext(sc, 1)

# direct kafka stream for users records corresponding to the topic or hashtag
directKafkaStream = KafkaUtils.createDirectStream(ssc,cfg['kafka']['topic']['twitter'],{"metadata.broker.list":",".join(cfg['kafka']['broker_list'])})

# function to write the timestamp and distinct count of records in each rdd into cassandra
def get_output(rtime,rdd):
    rtime = rtime.replace(tzinfo=from_zone)
    localrtime = rtime.astimezone(to_zone)
    localrtimestr = localrtime.strftime("%Y-%m-%d %H:%M:%S")
    reachcount = rdd.distinct().count()
    session.execute(
    """
    INSERT INTO StreamingReach(reachtime,reach)
    VALUES (%s, %s)
    """,
    (localrtimestr,reachcount)
    )
 
lines = directKafkaStream.map(lambda x: x[1]).map(lambda text: ''.join(i for i in text if ord(i)<128))
users = lines.map(lambda line: line.split(",")).map(lambda line: line[2])
count = users.foreachRDD(get_output) 
        

ssc.start()
ssc.awaitTermination()
