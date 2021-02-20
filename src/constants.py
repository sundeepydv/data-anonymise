#from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

#spark_session = SparkSession.builder.enableHiveSupport().getOrCreate

conf = SparkConf()
sc = SparkContext.getOrCreate(conf=conf)

PATH = 'date/'