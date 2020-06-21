import sys
sys.path.append('../spark_lineage')

import pyspark.sql.functions as f
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import json

from spark_lineage.LineageFactory import LineageFactory
from spark_lineage.domain.Parser import Parser

lineage = LineageFactory()

spark = SparkSession.builder.appName("spark-lineage").getOrCreate()

df = spark.createDataFrame(["10","11","13"], "string").toDF("age")
df2 = spark.createDataFrame(["10","11","13"], "string").toDF("age")

df.registerTempTable('zippo')
df2.registerTempTable('zap')

@lineage.lineage()
def extract_cat():
    return spark.sql('SELECT zippo.age, k.age FROM zippo LEFT JOIN zap as k on k.age = zippo.age')

extracted = extract_cat()
print(extracted.require)

