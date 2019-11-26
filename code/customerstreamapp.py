from __future__ import print_function
import pika
import sys
import json
import pyspark
import time
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler
import pandas as pd
import multiprocessing
import threading

database_features_ordered = ['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count','trip_distance','RatecodeID','store_and_fwd_flag','PULocationID','DOLocationID','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount']
sc = pyspark.sql.SparkSession.builder.appName("nycApp").getOrCreate()
sc.sparkContext._conf.set('spark.executor.cores', multiprocessing.cpu_count())
print(sc.sparkContext._conf.getAll())
lm = LinearRegressionModel()
model_1 = lm.load("/home/gcpkey/lr.model")
topic = "streaming_data"
credentials = pika.PlainCredentials('user', 'QwwyqaQj1C4i')
parameters = pika.ConnectionParameters('35.247.117.124',5672,'/',credentials)
connection = pika.BlockingConnection(parameters)
connection1 = pika.BlockingConnection(parameters)
channel = connection.channel()
channel1 = connection1.channel()
channel1.queue_declare(queue="receivePredictedFareClient1")
channel.queue_declare(queue=topic)
def callback(ch, method, properties, body):
    df_message = pd.DataFrame.from_dict([json.loads(body.decode())])
    df_message = df_message[database_features_ordered]
    df_message_pyspark = sc.createDataFrame(df_message)
    df_message_pyspark.write.csv("hdfs://cluster-9bfd-m/hadoop/data1.csv", header=True, mode='append')
    start = time.time()
    vectorAssembler = VectorAssembler(inputCols = ['VendorID', 'passenger_count', 'trip_distance', 'RatecodeID'], outputCol = 'features')
    vhouse_df = vectorAssembler.transform(df_message_pyspark)
    vhouse_df = vhouse_df.select(['features', 'fare_amount'])
    preds = model_1.transform(vhouse_df)
    predicted_fare = preds.toPandas()["prediction"][0]
    end = time.time()
    print("Time to run the prediction:" + str(end-start))
    preds.show()
    channel1.basic_publish(exchange='', routing_key="receivePredictedFareClient1", body=json.dumps({"fare_pred": predicted_fare, "prediction_time": end-start}))

channel.basic_consume(queue=topic, on_message_callback=callback, auto_ack=True)
print(' [*] Waiting for messages on topic', topic, 'To exit press CTRL+C')
channel.start_consuming()
