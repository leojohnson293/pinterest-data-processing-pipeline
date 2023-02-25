
# **Pinterest Data Processing Pipeline**


<!-- ### _Lambda Architecture_ -->
This project will aim to replicate the data processing pipeline that Pinterest uses to collect data from their users to run eperiments on what features to include and to drive business decisions to imporve their experience for their millions of users. The data processing pipeline they use is a ***lambda architecture*** as seen in the following Figure. This pipeline shows a data source with a data ingestion tool, sending data to a batch processing layer and a real-time processing layer(also know as the streaming layer ) and sent to a database to be queried using a query lanaguage such as SQL and sent to an analytics dashboard.
<p align="center">
<img src= "https://www.cuelogic.com/wp-content/uploads/2021/06/lambda-architecture-to-streaming-1.jpg" width=650>
<p>
<p align="center">
Figure 1 - Lamda Architecture
<p>
This data source ingests data from either the interactions on their APP or a developed API and send it for either batch or streaming processing as seen in Figure 1. An example of a tool used for a data ingestion which is used on this project is Apache Spark. Others include TIBCO , Flume and Sqoop.

In the batch processing layer, data is collected over time where it is already stored and processed in bulk. Then, the data is sent for processing either at regular intervals triggered by an orchestration tool such as Apache Airflow, when a certain criteria is met  or manually performed. Datesets used for batch processing could be huge(sometimes in terabyte or petabytes), so it is only meant for information that are not time-sensitive and results are not needed instantly.

In the real-time processing or streaming layer, the data is processed as soon as the records arrive. This is useful for applications where live update are needed such so changes can be added instaneously as soon as the data arrives. Because of this streaming can be used when the data is time-sensitive. Nowadays data processing has progressed from batch processing of data towards working with stream processing, however there are some situations where streaming is not appropriate such as when any type of historical analysis is needed or complex transformations are required before deriving insights from the data.

---
## **Data Ingestion**
### **FastAPI**

 FastAPI is a morder, high-performance web framework allows for fast and easy construction of APIs that is on par with NodeJS and Go. To run it, the web server will listen to requests made to the computer which will be passed on to FastAPI which will then return a response. It is combined with pydantic, which is used to assert the data types of all incoming data to allow for easier processing later on. The web server used to listen to the requests is ran locally using uvicorn, a library for ASGI server implementation.

The follwowing code is from the project_pin_API.py file which shows hows the uvicorn server is setup to listen to the requests from a user making post requests to Pinterest which is stimulated by the user_posting_emulation.py file. 
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

Here is the part of the code in the user_posting_emulation.py(not included in the repostitory due to securtiy reasons) file which stimulates a user making post requests ot Pinterest. The `AWSDBConnector` class (the methods of which are not included in the  due to securtiy reasons) is used to connect an RDS database which contains the pinterest data. The `run_infinite_data_loop` fucntion will be called to print out the data to the API.
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
<img src= "data:image/png;base64,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" width=650>
<p>
<p align="center">
Figure 2 - Kafka Architecture
<p>
To ingest the data, Apache Kafka is used which is an open-source tool used for data ingestion. Kafka uses three components. The producer, the consumer and the topic. The topic is where the data as records is stored. The producter writes the records into the topic that is inside the Kafka cluster and the consumer reads it from the topic. These components can be seen in the diagram in Figure 2.  Kafka is used in a Linux enviroment sucha as Ubuntu and uses java 1.8 . To ran Kafka, Zookeeper must be ran from the terminal in the kafka folder using the following command. Zookeeper is a 3rd party software that is there to act like a centralised service for Kafka and is used to keep track of Kafka topics and partitions.

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```
The next step is to next is to connect to a Kafka broker. A broker is a server that is part of the Kafka system. They store the topic log partitions and handle all the requests from clients such as produce, consume and metadata. Mulitple brokers makes a Kafka cluster. So, to start the server the following bash command in ran in a new terminal in the same folder while Zookeeper is kept open.
```bash
bin/kafka-server-start.sh config/server.properties
```
#### ***Kafka Topic***
So the next step is to create the Kafka topic. This is done by running the command below, in a new terminal while both Zookeeper and the broker are kept open.
 ```bash
bin/kafka-topics.sh --create --topic pinterest--partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```
Here a topic called "pinterest" is created, where all the records from the FastAPI will be stored. The next step is to use python to setup the producer the send the records to the topic as well as the consumer to read them. To do this the kafka-python library will be imported to the python script and will be installed by running the following command in the terminal.
```bash
pip install kafka-python
```

#### ***Kakfa Producer***
A producer is an API that enables another tool or an application to publishes/writes data to one or more Kafka topics. It provides data that Kafka will then ingest. The producer API comes built-in with Kafka and can be configured to connect to a wide variety of data sources. In python, data can be sent to the kafka topic using the `KafkaProducer` class from the kafka-python library that was previously installed from pip. The code below shows `run_infinite_post_data_loop` function from the FastAPI after edited to incoporate the Kafka producer, so it sends the data from the pinterest API after encoding it as bytes, to the "pinterest" topic created earlier.
```Python
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
A consumer is  an API that's responsible for reading data records from one or more Kafka topics. It provides the data from the kafka topic that can be sent to for batch or stream processing as messages. The code below shows how python accesses the consumer using the `KafkaConsumer` class from the kafka-python library when sending the messages the the streaming layer. Here, the consumer is initialized in the `__init__` method of the `Streaming_Layer` class as `self.consumer`. Then in the `get_message` method the value in each message which is the records sent to the topic from the API, is decoded using the lambda function in the `value_deserializer` in `self.consumer` and printed out using a for loop.
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
The first stage in the batch processing is to send the values for each message in the consumer as individual JSON files to an S3 bucket which is initialized as `self.BUCKET_NAME` in the `__init__` method. To do this, the boto3 python library is used. The code below shows the `send_to_S3` method of the `Batch_Layer` class, which will is what will send the message to S3. Firstly, the client is setup and in the code it is a variable called `s3`. The for loop is created to access the messages in the comsumer. In the for loop, the variable `folder` is created to access the `unique id` of each value in the messages, which will be used to name each JSON file using an f-string. Finally, the `put_object` function of the client to send to S3. It takes three arguement which are the body, bucket and the key. The bucket is given as the `self.BUCKET_NAME` which is the name of the S3 bucket initialized earlier. The key will be an f-string for the names of each file, using the folder variable to name them by the unique id. Then the body will take the values of the messages and uses `json.dumps` to return a JSON object that can be sent to S3 as JSON files.  
```Python
    def send_to_S3(self):
        s3 = boto3.client('s3')
    
        for msg in self.consumer:
            folder = msg.value['unique_id']
            s3.put_object(Body = (json.dumps(msg.value).encode("ascii")), Bucket = self.BUCKET_NAME ,Key = f'pinterest_data_{folder}.json') 
```
### **Apache Spark**
The next phase of the batching processing is using Apache Spark to process the the data sent to the S3 bucket. Spark is the industry standard tool for big data processing. Due to shear amount of data produced within a short timeframe, a library such as pandas will not be enough to process the data. One reason is that, the data may not even fit in the machine. Spark can process data distrubuted across different machines. Python has a python library that can interact with Spark called pyspark. To use pyspark, the following packages from the code below are imported into the python script. 

```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
```
- `SparkContext` is the object used by any driver to communicate with the cluster manager, execute and coordinate jobs
- `SparkConf` provides configurations to run a Spark application. 
- `SparkSession` is the entry point to programming Spark with the Dataset and DataFrame API 

The code below shows the `batch` method which is used to run the spark job. In the method, the spark configurations are set in the `cfg` variable and used to create the `SparkContext` which is set as `sc`. This is then used to create the `spark` variable which is the `SparkSession` used to run Spark.

Now to connect to S3, packages must be submitted to the pyspark shell from the Mavern repository. The pyspark shell is represented in the code as `os.environ["PYSPARK_SUBMIT_ARGS"]`. The packages are represented by the Mavern coordinates and the coordinates that are submitted to the pyspark shell are for the AWS S3 package and the Hadoop package which are separated by a comma in `os.environ["PYSPARK_SUBMIT_ARGS"]`.  

Then the next step is to configure the Hadoop configuration in a variable called `hadoopConf` using the `hadoopConfiguration` object. This is to setup the connection to the S3 bucket. Here the access key ID and the secret access key for the IAM role on AWS as given as arguement in the second and third line. Then in teh last line of the Hadoop configuration is where the credentials provider is set to make the connection from Spark to S3.

Once the connection is made, Spark can read all the JSON files in the bucket using `read.json` in the code and after that any transformation to clean the data either by removing or replacing erroneous variables or changing a column to the correct data type can be computed. 
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
Now that Spark can process the data from the S3 bucket, it needed to be triggered at a given time. This can be done using a task-orchestration such as Apache Airflow. In their original pipeline, Pinterest used their own tool called Pinball to, trigger the Spark jobs bu since then Pinball has been deprecated and replace with Airflow which has taken over as the industry standard tool for orchestrating tasks. Airflow allows the user to define a series of tasks to be executed in a specific order using the Airflow scheduler. In Airflow you use Directed Acyclic Graphs (DAGs) to define a workflow. The DAGs contain nodes that corresponds to a task and will be connected to one another. To start the server for Airflow the follwing command is run:
```bash
airflow webserver --port 8080
```
Then to moniter the DAGs, the scheduler needs to be run by running this following command in the terminal:
```bash
airflow scheduler
```
Once this command is run, the Airflow UI can be accessed by visiting `localhost:8080` in the browser. 

In python, Airflow can be accessed by importing to the script. The python script must must run in the folder called dags that is created inside the installed airflow folder. Here the Airflow module can run tasks using the `BashOperator` which schedules bash command to the DAG or the `PythonOperator`, which can run scheduled python functions. In the code below, the `PythonOperator` is used to define the tasks that run both the `send_to_S3` and `spark` methods from the `Batch_Layer` class. Both methods are triggered daily by setting hte `schedule_interval` to `'@daily'`. Lastly the task dependencies are set at the bottom of the code shown by `S3_task >> Spark_task`. This will create the sequence in which each task is triggered.  
```Python
with DAG('pinterest_dag',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    S3_task = PythonOperator(                      # Run the task to send the data to the S3 bucket.  
        task_id= 'send_to_S3',
        python_callable=send_to_S3
        ) 

    Spark_task = PythonOperator(                   # Run the task to send the data to the Spark dataframe.
        task_id='batch',
        python_callable=spark
    )

    S3_task >> Spark_task                          # Defines the sequence in which the tasks are run.
```
---
## ***Stream Processing***
### ***Spark Streaming***
### _Spark-Kafka Integration_
The code below is the `stream` method from the `StreamingLayer` class in the streaming_consumer.py file. Here the messages from the Kafka consumer are submitted to Spark streaming as separate batches. To connect kafka to spark streaming, packages are submitted to the pyspark shell from the Mavern repository likewise before with connect Spark to S3. The  Mavern coordinates  submitted to the pyspark shell here, are for the Kafka package and the postgresql package which are separated by a comma in `os.environ["PYSPARK_SUBMIT_ARGS"]`. The Kafka package is used to stream data from Kafka to Spark. The postgresql package is for later to send to stream data to a postgresql database.

So, to start the stream, first a spark session must be started similiar to the batch processing using `SparkSession`. Then, to get the stream, a variable called `stream_df` is created which is the data stream that will be read from the topic using the `readStream` function. Here the Kafka topic and bootstrap servers are given as arguements to connect the stream to the Kafka topic. 
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
The following code is the the schema for the Spark stream called `stream_schema`. This schema defines the structure of the dataframe foreach batch of the stream. The schema is constructed using the `StructType` class which is a collection of `StructField` constructors which used to define each column and their data types. All `StructField` constructors have the nullable variables set to `True`.
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
The next stage is to apply the transformations to the stream. First, the value part of the kafka message is selected and cast as a string. Then the `withColumn` function takes the values and applys the schema that was just created to structure the streamed dataframe. After that, the transformations used to the data for the batch processing are applied here. Finally the last line in the code below uses `na.drop` function which drops any rows where the values are null using the "follower_count" column as a reference.

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
After the streaming process from Kafka,  the streams are to be appended to a postgresql table defined as "experimental_data" in a database called "pinterest_streaming" using the Mavern coordinates submitted earlier to the pyspark shell. Pinterest uses the MemSQL database for their data storage after Spark streaming. However, MemSQL requires a license to be used, so instead PostgreSQL which is an open source SQL database that can be easily queried using pgadmin4 or SQLtools on VSCode is used. The code below shows the `for_each_batch` function that is created in the `stream` method which will be given to the `foreachBatch` function as an arguement. This will the data stream and the epoch ID for each batch as arguements. This function will configure the properties of the JDBC connection to postgresql database. The JDBC is a Java API that manages connections to a database. After the `writeStream` will be called to the `stream_df` stream to ouptub each batch to the JDBC connection will be append it to the "experimental_data" table on postgresql. 
```Python

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
```


