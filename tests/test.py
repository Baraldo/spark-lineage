import sys
sys.path.append('../spark_lineage')

import pyspark.sql.functions as f
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("spark-lineage").getOrCreate()

df = spark.createDataFrame(["10","11","13"], "string").toDF("age")

print(df.withColumn('age', f.lit(1))._jdf.queryExecution().logical().prettyJson())