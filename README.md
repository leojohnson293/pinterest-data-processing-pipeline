
# **Pinterest Data Processing Pipeline**


<!-- ### _Lambda Architecture_ -->
This project will aim to replicate the data processing pipeline that Pinterest uses to collect data from their users to run eperiments on what features to include and to drive business decisions to imporve the experience for their millions of users. The data processing pipeline they use is a ***lambda architecture*** as seen in Figure 1. This pipeline shows a data source with a data ingestion tool, sending data to a batch processing layer and a real-time processing layer(also know as the streaming layer) and which then sends it to a database to be queried using a query lanaguage such as SQL and sent to an analytics dashboard.
<p align="center">
<img src= "https://www.cuelogic.com/wp-content/uploads/2021/06/lambda-architecture-to-streaming-1.jpg" width=650>
<p>
<p align="center">
Figure 1 - Lamda Architecture
<p>
This data source ingests data from either the interactions on their APP or a developed API and sends it to batch or streaming processing as seen in Figure 1. An example of a tool used for a data ingestion which is used on this project is Apache Spark. Others include TIBCO , Flume and Sqoop.

In the batch processing layer, data is collected over time where it is already stored and processed in bulk. Then, the data is sent for processing either at regular intervals triggered by an orchestration tool such as Apache Airflow when a certain criteria is met or manually performed. Datesets used for batch processing could be huge(sometimes in terabyte or petabytes), so it is only used for information that are not time-sensitive and when the results are not needed instantly.

In the real-time processing or streaming layer, the data is processed as soon as the records arrive. This is useful for applications where live updates are needed such so changes can be added instaneously as soon as the data arrives. Because of this streaming can be used when the data is time-sensitive. Nowadays data processing has progressed from batch processing of data towards working with stream processing, however there are some situations where streaming is not appropriate such as when any type of historical analysis is needed or complex transformations are required before deriving insights from the data.

---
## **Data Ingestion**
### **FastAPI**

 FastAPI is a mordern, high-performance web framework allows for fast and easy construction of APIs that is on par with NodeJS and Go. To run it, the web server will listen to requests made to the computer which will be passed on to FastAPI and will then return a response. It is combined with pydantic, which is used to assert the data types of all incoming data to allow for easier processing later on. The web server used to listen to the requests is ran locally using uvicorn, a library for ASGI server implementation.

The following code is from the project_pin_API.py file which shows hows the uvicorn server is setup to listen to the requests from a user making post requests to Pinterest which is stimulated by the user_posting_emulation.py file. 
``` python
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

app = FastAPI()

class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str

@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    return item

if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)    
```

Here is the part of the code in the user_posting_emulation.py(not included in the repostitory due to securtiy reasons) file which stimulates a user making post requests on Pinterest. The `AWSDBConnector` class (the methods of which are not included in the  due to securtiy reasons) is used to connect to an AWS RDS database which contains the pinterest data. The `run_infinite_data_loop` fucntion will be called to print out the data to the API.
``` python
new_connector = AWSDBConnector()

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        selected_row = engine.execute(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
        for row in selected_row:
            result = dict(row)
            requests.post("http://localhost:8000/pin/", json=result)
            print(result)
```
### **Apache Kafka**
<p align="center">
<img src= "https://images.idgesg.net/images/article/2017/08/apache-kafka-architecture-100731656-large.jpg?auto=webp&quality=85,70" width=650>
<p>
<p align="center">
Figure 2 - Kafka Architecture
<p>

To ingest the data, Apache Kafka is used which is an open-source tool used for data ingestion. Kafka uses three components. The producer, the consumer and the topic. The topic is where the data is stored as records. The producter writes the records into the topic that is inside the Kafka cluster and the consumer reads it from the topic. These components can be seen in the diagram in Figure 2.  Kafka is used in a Linux enviroment such as Ubuntu and uses java 1.8. To ran Kafka, Apache Zookeeper must be ran from the terminal in the kafka folder . Zookeeper is a 3rd party software that is there to act like a centralised service for Kafka and is used to keep track of Kafka topics and partitions. The following command is used to start the zookeeper server.

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```
The next step is to next is to connect to a Kafka broker. A broker is a server that is part of the Kafka system. They store the topic log partitions and handle all the requests from clients such as produce, consume and metadata. Mulitple brokers make up a Kafka cluster. So, to start the server, the following bash command is ran in a new terminal in the same folder while Zookeeper is kept open.
```bash
bin/kafka-server-start.sh config/server.properties
```
#### ***Kafka Topic***
Now the next step is to create the Kafka topic. This is done by running the command below, in a new terminal while both Zookeeper and the broker are kept open.
 ```bash
bin/kafka-topics.sh --create --topic pinterest--partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```
Here a topic called "pinterest" is created, where all the records from the FastAPI will be stored. The next step is to use python to setup the producer to send the records to the topic, and then the consumer to read them. To do this the kafka-python library will be imported to the python script and will be installed by running the following command in the terminal.
```bash
pip install kafka-python
```

<!-- #### ***Kakfa Producer*** -->
A producer is the Kafka API that enables another tool or an application to publish/write data to one or more Kafka topics. It provides data that Kafka will then ingest. The producer API comes built-in with Kafka and can be configured to connect to a wide variety of data sources. In python, data can be sent to the kafka topic using the `KafkaProducer` class from the kafka-python library that was previously installed from pip. Here it is defined in the `producer` variable. The code below shows `run_infinite_post_data_loop` function seen is the FastAPI section after it is edited to **incoporated** the Kafka producer, so it sends the data from the pinterest API after encoding it as bytes <!--using the lambda function in the `value_deserializer`-->, to the "pinterest" topic.
```Python
producter = KafkaProducer(
   bootstrap_servers = "localhost:9092",
   #value_serializer = lambda x: json.dumps(x).encode('uft-8')
)

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        selected_row = engine.execute(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
        for row in selected_row:
            result = dict(row)
            requests.post("http://localhost:8000/pin/", json=result)
            producter.send('pinterest', json.dumps(result).encode('ascii'))
            producter.flush()
            print(result)
```
#### ***Kakfa Consumer***
A consumer is the Kafka API that's responsible for reading data records from one or more Kafka topics. It provides the data from the Kafka topic that can be sent for batch or stream processing as messages. The code below shows how python accesses the consumer using the `KafkaConsumer` class from the kafka-python library when sending the messages the streaming layer. Here, the consumer is initialized in the `__init__` method of the `Streaming_Layer` class as `self.consumer`. Then, in the `get_message` method the value in each message which is the records sent to the topic from the API, is decoded using the lambda function in the `value_deserializer` in `self.consumer` and printed out using a for-loop.
```Python
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
```
---
## ***Batch Processing***
Now the kafka consumer will read the messages for batch processing. In the batch_consumer.py, the `Batch_Layer` class is created and the `__init__` method initializes the consumer as `self.consumer` likewise with the streaming later as seen in the code above. Other variables have also been initialized that will be used later in the batch processing.   
```python
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
```
### **S3 Bucket**
The first stage in the batch processing is to send the values for each message in the consumer as individual JSON files to an S3 bucket which is initialized as `self.BUCKET_NAME` in the `__init__` method. To do this, the boto3 python library is used. The code below shows the `send_to_S3` method of the `Batch_Layer` class, which is what will send the messages to S3. Firstly, the client is setup and in the code it is a variable called `s3`. Then, a for-loop is created to access the messages in the comsumer. In the for-loop, the variable `folder` is created to access the `unique id` of each value in the messages, which will be used to name each JSON file using an f-string. Finally, the `put_object` function of the client is used to send the messages to S3. It takes three arguements which are the body, bucket and the key. The bucket is given as the `self.BUCKET_NAME` which is the name of the S3 bucket initialized earlier. The key will be an f-string for the names of each JSON file, using the folder variable to name them by the unique id. Then the body will take the values of the messages and uses `json.dumps` to return a JSON object that can be sent to S3 as JSON files.  
```Python
    def send_to_S3(self):
        s3 = boto3.client('s3')
    
        for msg in self.consumer:
            folder = msg.value['unique_id']
            s3.put_object(Body = (json.dumps(msg.value).encode("ascii")), Bucket = self.BUCKET_NAME ,Key = f'pinterest_data_{folder}.json') 
```
### **Apache Spark**
The next phase of the batching processing is using Apache Spark to process the the data sent to the S3 bucket. Spark is the industry standard tool for big data processing. Due to shear amount of data produced within a short timeframe, a library such as pandas will not be enough to process the data. One reason is that, the data may not even fit in the local machine. Spark can process data distrubuted across different machines. Python has a python library that can interact with Spark called pyspark. To use pyspark, the following packages from the code below are imported into the python script. 

```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
```
- `SparkContext` is the class used by any driver to communicate with the cluster manager, execute and coordinate jobs
- `SparkConf` provides configurations to run a Spark application. 
- `SparkSession` is the entry point to programming Spark with the Dataset and DataFrame API 

The code below shows the `batch` method which is used to run the spark job. In the method, the spark configurations are set in the `cfg` variable and used to create the `SparkContext` which is set as `sc`. This is then used to create the `spark` variable which is the `SparkSession` used to run Spark.

Now to connect to S3, packages must be submitted to the pyspark shell from the Mavern repository. The pyspark shell is represented in the code as `os.environ["PYSPARK_SUBMIT_ARGS"]`. The packages are represented by the Mavern coordinates and these coordinates that are submitted to the pyspark shell are for the AWS S3 package and the Hadoop package which are separated by a comma in `os.environ["PYSPARK_SUBMIT_ARGS"]`.  

Then the next step is to setup the Hadoop configuration in a variable called `hadoopConf` using the `hadoopConfiguration` class. This is to setup the connection to the S3 bucket. Here the access key ID and the secret access key for the IAM role on AWS as given as arguements in the second and third lines. Then in the last line of the Hadoop configuration is where the credentials provider is set to make the connection from Spark to S3.

Once the connection is made, Spark can read all the JSON files in the bucket using `read.json` into a Spark dataframe and after that any transformation to clean the data either by removing or replacing erroneous variables or changing a column to the correct data type can be computed. 
```Python
    def batch(self):
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.395,org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell" # Packages to connect to S3 and Hadoop from the maven repository
        cfg = SparkConf().setAppName('S3toSpark').setMaster('local[*]')  # Creates the Spark configuration      
        sc=SparkContext(conf=cfg)        # Creates the Spark context 
        spark=SparkSession(sc).builder.appName("S3toSpark").getOrCreate()  # Creates the Spark session

        # Configure the setting to read from the S3 bucket
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
```

### **Apache Airflow**
Now that Spark can process the data from the S3 bucket, it needed to be triggered at a given time. This can be done using a task-orchestration tool such as Apache Airflow. In their original pipeline, Pinterest used their own tool called Pinball to, trigger the Spark jobs but since then Pinball has been deprecated and replaced with Airflow which has taken over as the industry standard tool for orchestrating tasks. Airflow allows the user to define a series of tasks to be executed in a specific order using the Airflow scheduler. In Airflow you use Directed Acyclic Graphs (also refered to as DAGs) to define a workflow. The DAGs contain nodes that corresponds to a task and will be connected to one another. To start the server for Airflow the following command is ran:
```bash
airflow webserver --port 8080
```
Then to moniter the DAGs, the scheduler needs to be run by running this following command in the terminal:
```bash
airflow scheduler
```
Once this command is run, the Airflow UI can be accessed by visiting `localhost:8080` in the browser. 

In python, Airflow can be accessed by importing to the script. The python script must be run in the folder called DAGs that is created inside the installed airflow folder. Here the Airflow module can run tasks using the `BashOperator` which schedules bash command to the DAG or the `PythonOperator`, which can run scheduled python functions. In the code below shows the 'pinterest_dag' is created to schedule the tasks to send the data from kafka to the S3 bucket and loading them to a Spark dataframe. Here, the `PythonOperator` is used to define the tasks that run both the `send_to_S3` and `spark` methods from the `Batch_Layer` class. Also between these tasks, a task is created is created using the `BashOperator`, to run the bash command 'sleep 10' which is to pause the DAG for ten seconds. This is so the DAG pauses ten seconds to ensure all of the data is sent to S3 before starting the Spark_task. All of the methods are triggered daily by setting hte `schedule_interval` to `'@daily'`. Lastly the task dependencies are set at the bottom of the code shown by `S3_task >> sleep_task >> Spark_task`. This will create the sequence in which each task is triggered.  
```Python
with DAG('pinterest_dag',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    S3_task = PythonOperator(                      # Run the task to send the data to the S3 bucket.  
        task_id= 'send_to_S3',
        python_callable=send_to_S3                     
        )

    sleep_task = BashOperator(                     # Run the task to pause the dag. 
        task_id='sleep_task',
        bash_command='sleep 10'
    )


    Spark_task = PythonOperator(                   # Run the task to send the data to the Spark dataframe.
        task_id='batch',
        python_callable=spark
    )

    S3_task >> sleep_task >> Spark_task            # Defines the sequence in which the tasks are run.
```

---
## ***Stream Processing***
### ***Spark Streaming***
### _Spark-Kafka Integration_
The code below is the `stream` method from the `StreamingLayer` class in the streaming_consumer.py file. Here the messages from the Kafka consumer are submitted to Spark streaming as separate batches. To connect kafka to spark streaming, packages are submitted to the pyspark shell from the Mavern repository likewise before when Spark had to be connected to S3 for batch processing. The  Mavern coordinates  submitted to the pyspark shell here, are for the Kafka package and the PostgreSQL package which are separated by a comma in `os.environ["PYSPARK_SUBMIT_ARGS"]`. The Kafka package is used to stream data from Kafka to Spark. The PostgreSQL package is for later to send to stream data to a PostgreSQL database.

So, to start the stream,a spark session must be first started similiar to the batch processing using `SparkSession`. Then, to get the stream, a variable called `stream_df` is created which is the data stream that will be read from the topic using the `readStream` function. Here the Kafka topic and bootstrap servers are given as arguements to connect Spark streaming to the Kafka topic. 
```Python
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
```
### _Schema_
The following code is the the schema for the Spark stream called `stream_schema`. This schema defines the structure of the dataframe for each batch of the stream. The schema is constructed using the `StructType` class which is a collection of `StructField` constructors which used to define each column and their data types. All `StructField` constructors have the nullable variables set to `True`.
```Python
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
```
### _Streaming Transformations_
The next stage is to apply the transformations to the stream. First, the value part of the kafka message is selected and cast as a string. Then the `withColumn` function takes the values and applys the schema that was just created to structure the streamed dataframe. After that, the transformations used to the data for the batch processing are applied here. Finally the last line in the code below uses `na.drop` function which drops any rows where the values are null using the "follower_count" column as a reference. Thiis means these rows would be added to the PostgreSQL database.

```Python
        stream_df = stream_df.selectExpr("CAST(value as STRING)")
        stream_df = stream_df.withColumn("value", from_json("value", stream_schema))
        stream_df = stream_df.select('value.*')
        stream_df = stream_df.withColumn('tag_list', regexp_replace('tag_list', "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "None"))
        stream_df = stream_df.withColumn('image_src', regexp_replace('image_src', "Image src error.", "None"))       
        stream_df = stream_df.withColumn('follower_count', regexp_replace('follower_count', 'k', '000'))
        stream_df = stream_df.withColumn('follower_count', regexp_replace('follower_count', 'M', '000000'))
        stream_df = stream_df.withColumn("follower_count",stream_df.follower_count.cast(IntegerType()))
        stream_df = stream_df.na.drop(subset=["follower_count"])
```
After the streaming the data from the Kafka topic, the streams are to be appended to a PostgreSQL table defined as "experimental_data" in a database called "pinterest_streaming" using the Mavern coordinates submitted earlier to the pyspark shell. Pinterest normally uses the MemSQL database for their data storage after Spark streaming. However, MemSQL requires a license to be used, so instead PostgreSQL which is an open source SQL database that can be easily queried using pgadmin4 or SQLtools on VSCode is used. The code below shows the `send_to_SQL` function that is created in the `stream` method which will be given to the `foreachBatch` function as an arguement. Here, the `stream_df` data stream and the epoch ID for each batch are given as arguements to the `send_to_SQL` function. This function will configure the properties of the JDBC connection to PostgreSQL. The JDBC is a Java API that manages connections to a database. After setting uo the the `writeStream` will be called to the `stream_df` stream to ouptub each batch to the JDBC connection will be append it to the "experimental_data" table on PostgreSQL. 
```Python

        def send_to_SQL(stream_df,epoch_id):

            url = 'jdbc:postgresql://localhost:5432/pinterest_streaming'
            properties = {"user" : "postgres", "password": "password", "driver":"org.postgresql.Driver"}
            stream_df.write.jdbc(url=url , table="experimental_data" , mode = 'append' , properties=properties)


        stream_df.writeStream \
            .format("jdbc") \
            .foreachBatch(send_to_SQL) \
            .outputMode("update")\
            .start()\
            .awaitTermination()
```


