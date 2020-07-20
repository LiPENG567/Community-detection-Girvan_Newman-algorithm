#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pyspark
from itertools import permutations
import itertools
import time
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import os
from graphframes import GraphFrame
from pyspark.sql.functions import col, lit, when
import sys
filter_thresh = sys.argv[1]
input_path = sys.argv[2]
out_path = sys.argv[3]

f_sh = int(filter_thresh)
# os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11  pyspark-shell")
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11  pyspark-shell")
start = time.time()
sc = pyspark.SparkContext()
spark_sql = SparkSession(sc)

rdd = sc.textFile(input_path)
header = rdd.first()
rdd_d = rdd.filter(lambda a: a!= header)
Myrdd = rdd_d.map(lambda line: (line.split(',')[0], line.split(',')[1])).persist()
user_rddr = Myrdd.groupByKey()
user_busi_dict = user_rddr.map(lambda a: (a[0],list(a[1]))).filter(lambda a: len(a[1])>= f_sh).map(lambda a: {a[0]:a[1]}).flatMap(lambda a: a.items()).collectAsMap()
user_rdd = user_rddr.map(lambda a: a[0])
# user_df = user_rdd.map(lambda x: (x, )).toDF()

# create edge as dataframe 
def check3(dict1, dict2):
    ins_len = len(set(dict1).intersection(set(dict2)))
    if ins_len >= f_sh:
        return True
    else:
        return False
new_rddr = user_rdd.cartesian(user_rdd).filter(lambda a: a[0]< a[1]).filter(lambda a: check3(user_busi_dict[a[0]],user_busi_dict[a[1]]))
user = new_rddr.map(lambda a: list([a[0],a[1]])).flatMap(lambda a: a).distinct()
user_df = user.map(lambda x: (x, )).toDF(["id"])
edge_df = new_rddr.toDF(["src", "dst"])
# print(user.take(10))
# edge_df.show()

# user_df.show()
g = GraphFrame(user_df, edge_df)
# print(g)
result = g.labelPropagation(maxIter=5).persist()
# result.show()
res = result.select("id", "label").rdd.map(tuple).map(lambda a: (a[1],a[0])).groupByKey().map(lambda a: (a[0], tuple(sorted(list(a[1]))))).map(lambda a: (a[1][0], a[1])).sortByKey().map(lambda a: (len(a[1]),a[1])).sortByKey().map(lambda a: a[1]).collect() 
# print(res.take(50))
filename = out_path
with open(filename,'w') as zaili:
    for a in res:
        l = len(a)
        for i in range(l):
            if i == l-1:
                zaili.write(str(a[i]))
            else:
                zaili.write(str(a[i])+', ')
        zaili.write("\n")
            
    zaili.write("Candidates:"+ '\n')
end = time.time()
print("Duration", end-start)

