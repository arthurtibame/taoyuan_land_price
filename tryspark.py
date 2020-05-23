from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2")
sc = SparkContext(conf=conf)


spark = SparkSession.builder.appName("myApp1") \
.config("spark.mongodb.input.uri", "mongodb://35.221.171.163:27017/lin.Bade") \
.config("spark.mongodb.output.uri", "mongodb://35.221.171.163:27017/lin.Bade") \
.getOrCreate()

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
print(df.printSchema())
print(df.show())