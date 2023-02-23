# ***Pinterest Data Processing Pipeline***
## ***Project Background***
<!-- ### _Lambda Architecture_ -->
This project will aim to replicate the data processing pipeline that Pinterest uses to collect data from their users to run eperiments on what features to include and to drive business decisions to imporve their experience for their millions of users. The data processing pipeline they use is a ***lambda architecture*** as seen in the following Figure. 
<p align="center">
<img src= "https://upload.wikimedia.org/wikipedia/commons/1/14/Diagram_of_Lambda_Architecture_%28generic%29.png" width=550>
<p>
<p align="center">
Figure 1 - Lamda Architecture
<p>
This pipeline shows a data source sending data to a batch processing layer and a real-time processing layer(also know as the streaming layer) and sent to a database to be queried using a query lanaguage such as SQL. This data source ingests data from either the interactions on their APP or a developed API. An example of a tool used for a data source which is used on this project is Apache Spark. Others include TIBCO , Flume and Sqoop.

In the batch processing layer, data is collected over time and data is sent for processing either at regular intervals, when a certain criteria is met and triggered by an orchestration tool such as Apache Airflow, or when manually performed. 

<!-- Batch processing is widely used in organisations, as many of the legacy systems were built upon this philosophy of data engineering where:

    Data is collected over time
    The collected data is sent for processing either at regular intervals, when a certain criteria is met, or when manually performed
    Datasets could be huge (terabytes or petabytes in size) and processing this data can be time-consuming. Hence, it's meant for information that isn't very time-sensitive.

When does it make sense to do batch processing?

    You already have all of the data in storage
    Results are not time-sensitive
    Data migrations are required from one storage system to another (such as from on-premise hardware to cloud-based storage)

When does batch processing not make sense?

    When results are required instantly or in (near) real-time
    When data is constantly flowing in, and operations depend on up-to date results being shown as they arrive (for instance, Google maps) -->


```Python
```
---
## ***Data Ingestion***
```Python
```
---
## ***Batch Processing***
```Python
```
---
## ***Stream Processing***
```Python
```
```Python
```
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
```Python
with DAG('pinterest_dag',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    S3_task = PythonOperator(
        task_id= 'send_to_S3',
        python_callable=send_to_S3
        ) 

    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3'
    )

    Spark_task = PythonOperator(
        task_id='send_to_spark',
        python_callable=spark
    )

    S3_task >> downloading_data >> Spark_task
```