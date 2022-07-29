#!/bin/bash

start=$(date +%s.%N)

source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /spark-examples/lab2/Question3/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /spark-examples/lab2/Question3/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /spark-examples/lab2/Parking_Violations_Issued_-_Fiscal_Year_2022.csv /spark-examples/lab2/Question3/input/
/usr/local/spark/bin/spark-submit --conf spark.default.parallelism=2 --master=spark://$SPARK_MASTER:7077 ./tickets-issued.py hdfs://$SPARK_MASTER:9000/spark-examples/lab2/Question3/input/


end=$(date +%s.%N)
runtime=$(python -c "print(${end} - ${start})")

echo "Runtime was $runtime"

