import time
from kafka import KafkaConsumer
import os


class Consumer(object):

    def __init__(self, addr, topic):
        """Initialize Consumer with kafka broker IP, and topic."""
        print addr
        print topic
        self.temp_file_path = None
        self.temp_file = None
        self.hadoop_path = "/user/hduser/history"
        self.cached_path = "/user/hduser/cached"
        self.topic = topic
        self.block_cnt = 0
        self.consumer = KafkaConsumer(topic, addr)

    def consume_topic(self, output_dir):
 
        timestamp = time.strftime('%Y%m%d%H%M%S')
        
        # open file for writing
        self.temp_file_path = "%s/kafka_%s_%s.dat" % (output_dir,self.topic,timestamp)
        self.temp_file = open(self.temp_file_path,"w")

        while True:
            try:
                # get 1000 messages at a time, non blocking
                # OffsetAndMessage(offset=43, message=Message(magic=0,
                # attributes=0, key=None, value='some message'))
                for message in self.consumer:
                  self.temp_file.write(str(message.value) + "\n")

                # file size > 20MB
                if self.temp_file.tell() > 20000000:
                    self.flush_to_hdfs(output_dir)

                #self.consumer.commit()
            except:
                # move to tail of kafka topic if consumer is referencing
                # unknown offset
                print "error"
                #self.consumer.seek(0, 2)


    def flush_to_hdfs(self, output_dir):
        """Flushes the 20MB file into HDFS.
        Code template from https://github.com/ajmssc/bitcoin-inspector.git
        Flushes the file into two folders under
        hdfs://user/PuppyPlaydate/history and
        hdfs://user/PuppyPlaydate/cached
        Args:
            output_dir: string representing the directory to store the 20MB
                before transferring to HDFS
        Returns:
            None
        """
        self.temp_file.close()

        timestamp = time.strftime('%Y%m%d%H%M%S')


        hadoop_fullpath = "%s/%s_%s_%s.dat" % (self.hadoop_path,
                                               self.topic, timestamp)
        cached_fullpath = "%s/%s_%s_%s.dat" % (self.cached_path,
                                               self.topic, timestamp)
        print "Block {}: Flushing 20MB file to HDFS => {}".format(str(self.block_cnt),
                                                                  hadoop_fullpath)
        self.block_cnt += 1

        # place blocked messages into history and cached folders on hdfs
        os.system("sudo -u hdfs hdfs dfs -put %s %s" % (self.temp_file_path,
                                                        hadoop_fullpath))
        os.system("sudo -u hdfs hdfs dfs -put %s %s" % (self.temp_file_path,
                                                        cached_fullpath))
        os.remove(self.temp_file_path)

        timestamp = time.strftime('%Y%m%d%H%M%S')

        self.temp_file_path = "%s/kafka_%s_%s_%s.dat" % (output_dir,
                                                         self.topic,
                                                         timestamp)
        self.temp_file = open(self.temp_file_path, "w")


if __name__ == '__main__':

    print "\nConsuming messages..."
    cons = Consumer("localhost:9092","twitter_test")
    cons.consume_topic("/home/ubuntu/Ingestion/kafka_messages")
