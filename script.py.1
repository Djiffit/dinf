from pyspark import SparkContext, SparkConf
import os
import sys

#datasets path on shared group directory on Ukko2. Uncomment the one which you would like to work on.
dataset = "/proj/group/distributed-data-infra/data-1-sample.txt"
#dataset = "/proj/group/distributed-data-infra/data-1.txt"
#dataset = "/proj/group/distributed-data-infra/data-2-sample.txt"
#dataset = "/proj/group/distributed-data-infra/data-2.txt"

conf = (SparkConf()
        .setAppName("konstaku")           ##change app name to your username
        .setMaster("spark://128.214.48.227:7077")
        .set("spark.cores.max", "10")  ##dont be too greedy ;)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true"))

sc = SparkContext(conf=conf)

data = sc.textFile(dataset)
data = data.map(lambda s: float(s))

set(data.map(lambda x: int(x)))

count = data.count()
sum = data.sum()
count = data.count()

print "Count = %.8f" % count
print "Sum = %.8f" % sum