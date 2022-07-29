from __future__ import print_function
import sys
from pyspark.sql import SparkSession
import math
from math import sqrt
from collections import Counter
from collections import defaultdict
from operator import itemgetter

def nearestCenter(d, centroids):
        distance_list = []
        for c in centroids:
                val = 0.
                for i in range(3):
                        val += (d[i] - c[i]) ** 2
                distance = sqrt(val)
                distance_list.append(distance)
        nearest = float('inf')
        index = -1
        for i, j in enumerate(distance_list):
                if j < nearest:
                        nearest = j
                        index = i
        return int(index), d


def calcCentroid(d):
        key, value = d[0], d[1]
        n = len(value)
        update = [0.] * 3
        for i in value:
                update[0] += float(i[0])
                update[1] += float(i[1])
                update[2] += float(i[2])
        nCenter = [round(x / n, 4) for x in update]
        return nCenter

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("streetCodesKmeans")\
        .getOrCreate()


    df = spark.read.format("csv").load(sys.argv[1], header =True, inferSchema =True)

    black_names_list = ['BK','BLACK', 'BLK', 'BK/', 'BLAC', 'Black', 'BCK','BKBK','BLAK']
    black = df.filter(df['Vehicle Color'].isin(black_names_list))
    dataPoints = black.select(black['Street Code1'], black['Street Code2'], black['Street Code3']).na.drop()
    dataRDD = dataPoints.rdd.map(lambda r: (r[0], r[1], r[2]))

    k = 4
    starting_centroids = dataRDD.takeSample(False, k)
    iteration = 0
    old_centroids = starting_centroids

    for m in range(40):
        mapper1 = dataRDD.map(lambda x: nearestCenter(x, old_centroids))
        reducer1 = mapper1.groupByKey()
        mapper2 = reducer1.map(lambda x: calcCentroid(x)).collect()
        updated_centroids = mapper2
        converge = 0
        for i in range(k):
            if updated_centroids[i] == old_centroids[i]:
                converge += 1
            else:
                difference = 0.0009
                closeDiff = [round((a - b)**2, 6) for a, b in zip(updated_centroids[i], old_centroids[i])]
                if all(v <= difference for v in closeDiff):
                    converge += 1
        if converge >= 4:
            break
        else:
            iteration += 1
            old_centroids = updated_centroids

    street_codes = [34510, 10030, 34050]
    near = nearestCenter(street_codes, updated_centroids)
    mapper3 = dataRDD.filter(lambda x: nearestCenter(x, updated_centroids)[0] == near[0]).collect()
    count = len(mapper3)
    token = dict(Counter(mapper3))
    counter = len(token)
    maxV = max(token.items(),key = itemgetter(1))[1]
    probability = round(count/(maxV * counter), 6)
    print ("The probability of getting a ticket is:\n")
    print (probability)

    spark.stop()
