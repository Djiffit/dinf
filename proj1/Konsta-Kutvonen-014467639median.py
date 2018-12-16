from pyspark import SparkContext, SparkConf
import os
import sys
import math

#datasets path on shared group directory on Ukko2. Uncomment the one which you would like to work on.
#dataset = "/proj/group/distributed-data-infra/data-1-sample.txt"
dataset = "/proj/group/distributed-data-infra/data-1.txt"
#dataset = "/proj/group/distributed-data-infra/data-2-sample.txt"
#dataset = "/proj/group/distributed-data-infra/data-2.txt"

conf = (SparkConf()
        .setAppName("konstaku")           ##change app name to your username
        .setMaster("spark://128.214.48.227:7077")
        .set("spark.cores.max", "5")  ##dont be too greedy ;)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true"))

sc = SparkContext(conf=conf)
data = sc.textFile(dataset)
data = data.map(lambda s: (float("{0:0.1f}".format(float(s))), float(s)))


# Group numbers by class and sort them by keys
rdd = data.groupByKey().sortByKey()
count = data.count()

# Map to get how many items each class contains
countMap = {}

total = 0
median = count / 2

# Save frequencies for each class
counts = data.countByKey()

for i in counts.items():
    countMap[i[0]] = i[1]

result = None
firstMedian = None

# Get keys to iterate over each class
keys = rdd.keys().collect()

# Function to get the values for a single class
def getOneClass(key):
    return sorted(rdd.filter(lambda x: x[0] == k).collect()[0][1])

# Iterate over all classes, counting the cumulative number of elements found
for k in keys:
    valCount = countMap[k]
    
    # Check if we are in the class that contains the median or half of it
    if total < median and math.floor(median) <= total + valCount:
        sort = getOneClass(k)
        
        # Median is single value
        if count % 2 == 1:
            result = sort[median - total]
            break
        else:
            # Median is split between two different classes
            if math.floor(median) == total + valCount:
                firstMedian = sort[int(median -0.5) - total]
                
                # Median is the average of two values in this class
            else:
                result = (sort[int(median - 0.5) - total]  + sort[int(median + 0.5) - total]) / 2
                break
    
        # First part of median was the last element of previous class and second part is first of this class
        if firstMedian != None and math.ceil(median) == total + 1:
            sort = getOneClass(k)
            result = (firstMedian + sort[0]) / 2
            break

    total = total + valCount

print("Count = %.8f" % count)
print("The median is ... ... ... ", result)
