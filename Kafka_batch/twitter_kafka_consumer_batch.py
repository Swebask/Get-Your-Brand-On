import time
import os
from kafka import KafkaConsumer
from py2neo import Graph


class Consumer(object):

    def __init__(self, topic):
        #Initialize Consumer with kafka broker IP, and topic.
        self.file_path = None
        self.temp_file = None
        self.topic = topic
        self.block_cnt = 0
        self.consumer = KafkaConsumer(topic, bootstrap_servers=addr)

    def consume_topic(self):
 
        timestamp = time.strftime('%Y%m%d%H%M%S')
        # open file for writing
        self.file_path = "%s/kafka_%s.csv" % (self.topic,timestamp)
        self.temp_file = open(self.file_path,"w")

        while True:
            try:
                self.temp_file.write("TweetId,CreatedAt,Tweet,ScreenName,FollowerCount\n")
                for message in self.consumer:
                    
                    self.temp_file.write(str(message.value))

                # file size > 20MB
                if self.temp_file.tell() > 20000000:
                    self.open_new_file()

            except:
                print ("error")

    def open_new_file(self):
        """closes the 20MB file.
        """
        self.temp_file.close()

        timestamp = time.strftime('%Y%m%d%H%M%S')

        self.file_path = "%s/kafka_%s.dat" % (self.topic,timestamp)
        self.temp_file = open(self.file_path, "w")


if __name__ == '__main__':

    print ("\nConsuming messages...")
    with open("consumerconfig.yml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
    broker_list = cfg['kafka']['broker_list']
    cons = Consumer(broker_list,cfg['kafka']['topic']['user'])
    cons.consume_topic()

