from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *
import json
import yaml
import boto3
import multiprocessing
import pyspark
import os



class Batch_Layer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            "pinterest",
            bootstrap_servers = "localhost:9092",
            value_deserializer = lambda msg: json.loads(msg.decode('ascii')),
            auto_offset_reset = "earliest"
        )
        self.BUCKET_NAME = 'pinterest-data-afc4779f-6007-47fc-90c9-e84f59c36905'
        self.keys = r'/home/leo/Desktop/AiCore/leo_accessKeys.yaml'
        with open(self.keys) as f:
            read_yaml = yaml.safe_load(f)
        self.accessKeyId=read_yaml['Access key ID']
        self.secretAccessKey=read_yaml['Secret access key']

    def send_to_S3(self):
        s3 = boto3.client('s3')
    
        for msg in self.consumer:
            folder = msg.value['unique_id']
            s3.put_object(Body = (json.dumps(msg.value).encode("ascii")), Bucket = self.BUCKET_NAME ,Key = f'pinterest_data_{folder}.json') 

    def batch(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.395,org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"
        cfg = SparkConf().setAppName('S3toSpark').setMaster('local[*]')         
        sc=SparkContext(conf=cfg)
        spark=SparkSession(sc).builder.appName("S3toSpark").getOrCreate()

        hadoopConf = sc._jsc.hadoopConfiguration()
        hadoopConf.set('fs.s3a.access.key', self.accessKeyId)
        hadoopConf.set('fs.s3a.secret.key', self.secretAccessKey)
        hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')


        df = spark.read.json("s3a://pinterest-data-afc4779f-6007-47fc-90c9-e84f59c36905/*.json")
        df = df.withColumn('tag_list', regexp_replace('tag_list', "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "None"))
        df = df.withColumn('follower_count', regexp_replace('follower_count', 'k', '000'))
        df = df.withColumn('follower_count', regexp_replace('follower_count', 'M', '000000'))
        df = df.withColumn('save_location', regexp_replace('save_location', 'Local save in ', ''))
        df = df.withColumn("follower_count",df.follower_count.cast(IntegerType()))
        df.show(200, truncate = True)
       
        




if __name__ == '__main__':
    read_data=Batch_Layer()
    read_data.batch()








