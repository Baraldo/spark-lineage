# Spark-Lineage
Spark-Lineage is a tool for data lineage, transformation mapping & business rule dependency for pyspark.

## Simple Example
```python
from spark_lineage.LineageFactory import LineageFactory
lineage = LineageFactory(Parser.INFER_PRODUCED)

@lineage.lineage(description='I create a a new column that is the same as AGE column.')
def return_df(df):
    return df.withColumn(colName = 'df', col = f.col('age'))

@lineage.lineage(is_extractor=True, description='This is a sample extractor, next time try me with a path!')
def extract_df():
    return spark.createDataFrame(["10","11","13"], "string").toDF("age")

@lineage.lineage(description='I am here to check my graphs!')
def return_df_again(df):
    return df.withColumn(colName = 'duf', col = f.col('df'))

df = extract_df(path=None)
df = return_df(df)
df = return_df_again(df)
print(df.graph())
```

## Install

## License