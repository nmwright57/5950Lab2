from __future__ import print_function
import sys
import math
from math import sqrt
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *



def nearest_center(d, centroids):
        distance_list = [] #dist_list = []
        for c in centroids:
                value = 0.
                for i in range(3):
                        value = value + (d[i] - c[i]) ** 2
                distance = sqrt(value)
                distance_list.append(distance)
        nearest = float('inf')
        index = -1
        for i, val in enumerate(distance_list):
                if val < nearest:
                        nearest = val
                        index = i
        return int(index), d





def calculate_new_centroid(d):
        key, value = d[0], d[1]
        n = len(value)
        updated_c = [0.] * 3
        for i in value:
                updated_c[0] = updated_c[0] + float(i[0])
                updated_c[1] = updated_c[1] + float(i[1])
                updated_c[2] = updated_c[2] + float(i[2])

        n_center = [round(x / n, 4) for x in updated_c]
        return n_center



if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("NBAKmeans")\
        .getOrCreate()


    df = spark.read.format("csv").load(sys.argv[1], header= True, inferSchema=True)
    dataPts = df.filter(df.player_name == 'lebron james').select('SHOT_DIST','CLOSE_DEF_DIST', 'SHOT_CLOCK').na.drop()
    dataRDD = dataPts.rdd.map(lambda r: (r[0], r[1], r[2]))


    k = 4
    starting_centroid = dataRDD.takeSample(False, k)

    iters = 0
    old_c = starting_centroid


    for m in range(50):
        first_mapper = dataRDD.map(lambda x: nearest_center(x, old_c))
        first_reducer = first_mapper.groupByKey()
        sec_mapper = first_reducer.map(lambda x: calculate_new_centroid(x)).collect() # collect a list
        new_c = sec_mapper
        converge = 0
        for i in range(k):
            if new_c[i] == old_c[i]:
                converge += 1

            else:
                diff = 0.0009
                closeDiff = [round((a - b)**2, 6) for a, b in zip(new_c[i], old_c[i])]
                if all(v <= diff for v in closeDiff):
                    converge += 1

        if converge >= 4:
            print("Converges at iteration %s\n" %(iters))
            print("\nFinal Centroids: %s" %(new_c))
            break

        else:
            iters += 1
            old_c = new_c



    spark.stop()

