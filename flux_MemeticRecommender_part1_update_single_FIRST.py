
# Calculate the average stars for each business category
# Written by Dr. Yuhang Wang for SI601
# To run on Fladoop cluster:
# First ssh into a login node, then launch pyspark shell by typing 'pyspark --queue si601w15'
# Copy & paste code below into the pyspark shell
'''
To run on Fladoop cluster
spark-submit --master yarn-client --queue si601w15 --num-executors 2 --executor-memory 1g --executor-cores 2 Test_project_review_wordCount_CHECK_MONTHS.py
MASTER=local[4] spark-1.2.0.2.2.0.0-82-bin-2.6.0.2.2.0.0-2041/bin/pyspark 
"hdfs://sandbox.hortonworks.com:8020/project/yelp_10k_months/month26_yelp_10k.txt"
'''
from scipy import spatial
import simplejson as json
from pyspark import SparkContext
import numpy as np
import cPickle as pickle
import re
sc = SparkContext(appName="UDU")
startMonth = 23
month = startMonth

input_file = sc.textFile("hdfs:///user/cdrayton/project/yelp_10k/month%s_yelp_FULL.txt"%month) 

lastMonth = sc.textFile('hdfs:///user/cdrayton/project/yelp_10k_outputs/month%s_yelp_10kCOUNTS.txt'%(month-1))

lastCount =lastMonth.map(lambda line: json.loads(line)).map(lambda x: ( (x[0][0],x[0][1]),x[1]))



WORD_RE = re.compile(r"\b[\w']+\b")
ACT_RE = re.compile(r"act_.+")
USE_RE = re.compile(r"use_.+")
def review_map_User(data):
    text = data.get('text', None)
    stars = data.get('stars', None)
    userID = data.get('user_id', None)
    userID = "use_" + str(userID)
    actionID = data.get('business_id', None)
    actionID = "act_" + str(actionID)
    return (text, (userID, actionID), stars)

reviewMap  = input_file.map(lambda line: json.loads(line))\
                      .map(review_map_User)
     
reviewCount = reviewMap.flatMap(lambda r: [((r[1], w),r[2] ) for w in WORD_RE.findall(r[0])])\
                      .flatMap(lambda a: (( (a[0][0][0],a[0][1]),a[1]) , ( (a[0][0][1],a[0][1]),a[1])))


reviewCount.map(lambda x: json.dumps(x)).saveAsTextFile("hdfs:///user/cdrayton/project/yelp_10k_outputs/month%s_yelp_10kCOUNTS.txt"%month)

for i in reviewCount.take(10):
    print i