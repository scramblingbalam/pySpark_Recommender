
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

import simplejson as json
from pyspark import SparkContext
sc = SparkContext(appName="UDU")

for month in range(39,40):
    input_file = sc.textFile("hdfs:///user/cdrayton/project/yelp_10k/month%s_yelp_FULL.txt"%month) 
    
    tfRecommendJson = sc.textFile("hdfs:///user/cdrayton/project/yelp_10k_outputs/month%s_yelp_10k_tfRANKING.txt"%(month-1))
    
    tf_recommend =tfRecommendJson.map(lambda line: json.loads(line)).map(lambda x: (tuple(x[0]),x[1]))
    
    
    
    def review_map_User(data):
        text = data.get('text', None)
        stars = data.get('stars', None)
        userID = data.get('user_id', None)
        userID = u"use_" + userID
        actionID = data.get('business_id', None)
        actionID = u"act_" + actionID
        return (text, (userID, actionID), stars)
    
    reviewMap  = input_file.map(lambda line: json.loads(line))\
                          .map(review_map_User)
                             
    recommendMap = reviewMap.map(lambda x: ((x[1][0],x[1][1]),x[2]))
    
    correlationTF = recommendMap.join(tf_recommend)
    
    correlationRecTF = correlationTF.map(lambda x: (x[0][0],(x[0][1],x[1][0],x[1][1])))\
                                    .join(tf_recommend.map(lambda x: (x[0][0], (x[0][1],x[1]))))#\
                                    #.map(lambda x: ((x[0],x[1][1][1]),(x[1][0][2],x[1][0][1],x[1][1][0],x[1][0][0])))\
                                    #.sortByKey()\
                                    #.reduceByKey(lambda a, b: a+b)
    for i in correlationRecTF.take(10):
        print
    #correlationTF.map(lambda x: json.dumps(x)).saveAsTextFile("hdfs:///user/cdrayton/project/yelp_10k_correlation/month%s_yelp_10k_correlationSummery_TF.txt"%month)
    #correlationRecTF.map(lambda x: json.dumps(x)).saveAsTextFile("hdfs:///user/cdrayton/project/yelp_10k_correlation/month%s_yelp_10k_CORRELATION_TF.txt"%month)
        
    for i in correlationTF.take(10):
        print "\n",i,"\n_____"

print "END_END_END\nEND_END_END\nEND_END_END" 





    
