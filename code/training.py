import pyspark

sc = pyspark.sql.SparkSession.builder.appName("nycApp").getOrCreate()

#from pyspark.ml.regression import LinearRegressionModel
#lm = LinearRegressionModel()
#model_1 = lm.load("/home/gcpkey/lr.model")
#exit()
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
lr_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="fare_amount",metricName="r2")
textFile = sc.read.csv("hdfs://cluster-9bfd-m/hadoop/data.csv", header=True, inferSchema=True)
vectorAssembler = VectorAssembler(inputCols = ['VendorID', 'passenger_count', 'trip_distance', 'RatecodeID'], outputCol = 'features')
vhouse_df = vectorAssembler.transform(textFile)
vhouse_df = vhouse_df.select(['features', 'fare_amount'])
splits = vhouse_df.randomSplit([0.8, 0.2])
train_df = splits[0]
test_df = splits[1]

from pyspark.ml.regression import LinearRegression
dt = LinearRegression(featuresCol ='features', labelCol = 'fare_amount')
dt_model = dt.fit(train_df)
dt_predictions = dt_model.transform(test_df)
dt_evaluator = RegressionEvaluator(labelCol="fare_amount", predictionCol="prediction", metricName="rmse")
rmse = dt_evaluator.evaluate(dt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
dt_model.save("/home/gcpkey/lr.model")
