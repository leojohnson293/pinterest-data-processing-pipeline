from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *
import multiprocessing
import pyspark
import os
import json

class Streaming_Layer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            "pinterest",
            bootstrap_servers = "localhost:9092",
            value_deserializer = lambda msg: json.loads(msg.decode('ascii')),
            auto_offset_reset = "earliest"
        )

    def get_message(self):
        for msg in self.consumer:
            print(msg.value)
    
    def stream(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.5.1 pyspark-shell'

        kafka_topic_name = "pinterest"
        kafka_bootstrap_servers = 'localhost:9092'

        sc = SparkSession \
                .builder \
                .appName("KafkaStreaming ") \
                .getOrCreate()
        

        stream_df = sc \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("subscribe", kafka_topic_name) \
                .option("startingOffsets", "earliest") \
                .load()
   
        stream_schema = StructType([
            StructField("index", IntegerType(), True),
            StructField("unique_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("poster_name", StringType(), True),
            StructField("follower_count", StringType(), True),
            StructField("tag_list", StringType(), True),
            StructField("is_image_or_video", StringType(), True),
            StructField("image_src", StringType(), True),
            StructField("downloaded", IntegerType(), True),
            StructField("save_location", StringType(), True),
            StructField("category", StringType(), True)
        ])


        stream_df = stream_df.selectExpr("CAST(value as STRING)")
        stream_df = stream_df.withColumn("value", from_json("value", stream_schema))
        stream_df = stream_df.select('value.*')
        stream_df = stream_df.withColumn('tag_list', regexp_replace('tag_list', "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "None"))
        stream_df = stream_df.withColumn('image_src', regexp_replace('image_src', "Image src error.", "None"))       
        stream_df = stream_df.withColumn('follower_count', regexp_replace('follower_count', 'k', '000'))
        stream_df = stream_df.withColumn('follower_count', regexp_replace('follower_count', 'M', '000000'))
        stream_df = stream_df.withColumn("follower_count",stream_df.follower_count.cast(IntegerType()))
        stream_df = stream_df.na.drop(subset=["follower_count"])



        def for_each_batch(stream_df,epoch_id):

            url = 'jdbc:postgresql://localhost:5432/pinterest_streaming'
            properties = {"user" : "postgres", "password": "password", "driver":"org.postgresql.Driver"}
            stream_df.write.jdbc(url=url , table="experimental_data" , mode = 'append' , properties=properties)


        stream_df.writeStream \
            .format("jdbc") \
            .foreachBatch(for_each_batch) \
            .outputMode("update")\
            .start()\
            .awaitTermination()

    
read_data=Streaming_Layer()
read_data.stream()


