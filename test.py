from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import expr
import pyspark.sql.functions as f
from spark_lineage.spark_lineage import produce, extract
spark = (SparkSession.builder.appName("test").getOrCreate())


@produce
def return_df(df):
    return df.withColumn(colName = 'df', col = f.col('age'))

@extract
def extract_df(alias):
    return spark.createDataFrame(["10","11","13"], "string").toDF("age")

df = extract_df(alias='kippo')

df = return_df(df)

print(df.lineage)