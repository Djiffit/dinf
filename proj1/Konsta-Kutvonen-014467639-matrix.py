from pyspark.mllib.linalg.distributed import IndexedRowMatrix, IndexedRow, DenseMatrix, BlockMatrix, MatrixEntry
from pyspark import SparkContext, SparkConf
import os
from pyspark import sql
import sys
import math
import numpy as np
from pyspark.sql import SparkSession, SQLContext
from operator import itemgetter

#datasets path on shared group directory on Ukko2. Uncomment the one which you would like to work on.
#dataset = "/proj/group/distributed-data-infra/data-1-sample.txt"
#dataset = "/proj/group/distributed-data-infra/data-1.txt"
#dataset = "/proj/group/distributed-data-infra/data-2-sample.txt"
dataset = "/proj/group/distributed-data-infra/data-2.txt"

conf = (SparkConf()
        .setAppName("konstaku")           ##change app name to your username
        .setMaster("spark://128.214.48.227:7077")
        .set("spark.cores.max", "10")  ##dont be too greedy ;)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true"))

sc = SparkContext(conf=conf)

# Open some context to allow for toDF function to work or something ??
sql.SQLContext(sc)
data = sc.textFile(dataset)

#data = (data.map(lambda s: (list(map(lambda x: float(x), s.split()))))).zipWithIndex().map(lambda x: ((x[1], 0), DenseMatrix(1, 1000, x[0])))

# Read matrix normally to format of (rownumber, vector)
data = data.map(lambda s: (list(map(lambda x: float(x), s.split())))).zipWithIndex().map(lambda x: (x[1], x[0]))

# Create a transpose for the matrix
tdata = sc.textFile(dataset).map(lambda s: list(map(lambda x: (x[0], float(x[1])), enumerate(s.split())))).zipWithIndex().flatMap(lambda x: map(lambda y: (y[0], (x[1], y[1])), x[0])).groupByKey()

# Map the transpose data to same format as normal matrix
tdata = tdata.map(lambda x: (x[0], map(lambda s: s[1], sorted(list(x[1]), key=itemgetter(0)))))

# Create BlockMatrix for the normal matrix and its transpose
mat = IndexedRowMatrix(data)
mat = mat.toBlockMatrix()
matTranspose = IndexedRowMatrix(tdata).toBlockMatrix()

# Get final result by multiplying mat * mat^T * mat
matTranspose = mat.multiply(matTranspose)
matRes = matTranspose.multiply(mat)

print('Done')
