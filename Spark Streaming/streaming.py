#Counting reach incrementally using spark stream

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from dateutil import tz
from datetime import datetime
import yaml
from cassandra.cluster import Cluster

cluster = Cluster(['ec2-52-205-142-108.compute-1.amazonaws.com'])
session = cluster.connect()
session.execute("USE reach;")


from_zone = tz.gettz('UTC')
to_zone = tz.gettz('America/New_York')

sc = SparkContext("local[3]", "ReachCount")
ssc = StreamingContext(sc, 1)


with open("streamconfig.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)
producer = KafkaProducer(bootstrap_servers=cfg['kafka']['broker_list'])


directKafkaStream = KafkaUtils.createDirectStream(ssc,cfg['kafka']['topic']['twitter'],{"metadata.broker.list":",".join(cfg['kafka']['broker_list'])})


def get_output(rtime,rdd):
    print "rtime"
    print rtime.strftime("%Y-%m-%d %H:%M:%S")
    rtime = rtime.replace(tzinfo=from_zone)
    localrtime = rtime.astimezone(to_zone)
    localrtimestr = localrtime.strftime("%Y-%m-%d %H:%M:%S")
    reachcount = rdd.count()
    session.execute(
    """
    INSERT INTO StreamingReach(reachtime,reach)
    VALUES (%s, %s)
    """,
    (localrtimestr,reachcount)
    )
    producer.send(cfg['kafka']['topic']['stream'],localrtimestr+','+str(reachcount)+'\n')
    print localrtimestr,str(rdd.count()),"record sent"
 
lines = directKafkaStream.map(lambda x: x[1]).map(lambda text: ''.join(i for i in text if ord(i)<128))
lines.pprint()
users = lines.map(lambda line: line.split(",")).map(lambda line: line[2])
count = users.foreachRDD(get_output) 
        

ssc.start()
ssc.awaitTermination()