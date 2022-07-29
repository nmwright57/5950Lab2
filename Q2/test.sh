#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /spark-examples/lab2/Question2/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /spark-examples/lab2/Question2/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /spark-examples/lab2/Parking_Violations.csv /spark-examples/lab2/Question2/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./streetCodesKmeans.py hdfs://$SPARK_MASTER:9000/spark-examples/lab2/Question2/input/
