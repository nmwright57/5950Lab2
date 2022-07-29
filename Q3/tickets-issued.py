from __future__ import print_function
import sys
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import Window

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("tickets-issued")\
        .getOrCreate()

    df = spark.read.format("csv").load(sys.argv[1], header =True, inferSchema =True)
    w = Window.partitionBy('Issue Date')
    df.groupBy('Issue Date').count().select('Issue Date', f.col('count').alias('count')).orderBy('count', ascending=False).show()


spark.stop()

