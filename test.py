from pyspark import SparkContext, SparkConf
from pyspark.sql import *
import pyspark.sql.functions as f
from spark_lineage.Lineage import Lineage


spark = (SparkSession.builder.appName("test").getOrCreate())

@Lineage.lineage(description='I create a a new column that is the same as AGE column.')
def return_df(df):
    return df.withColumn(colName = 'df', col = f.col('age'))

@Lineage.lineage(is_extractor=True, description='This is a sample extractor, next time try me with a path!')
def extract_df():
    return spark.createDataFrame(["10","11","13"], "string").toDF("age")

df = extract_df(path=None)
print(vars(df))
df = return_df(df)
print(vars(df))