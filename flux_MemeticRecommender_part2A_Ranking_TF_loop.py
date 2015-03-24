
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
from pyspark.mllib.feature import HashingTF


sc = SparkContext(appName="UDU")

for month in range(42,45):
    currentWordsJson = sc.textFile("hdfs:///user/cdrayton/project/yelp_10k_outputs/month%s_yelp_10kCOUNTS.txt"%month)
    
    currentWords = currentWordsJson.map(lambda line: json.loads(line)).map(lambda x: ((x[0][0],x[0][1]),x[1]))
    currentWord_list = currentWords.map(lambda x: (x[0][0], [x[0][1]]*x[1])).reduceByKey(lambda a, b: a + b)
    
    documents = currentWord_list.map(lambda x: x[1])
    Ids = currentWord_list.map(lambda x: x[0])
    
    hashingTF = HashingTF()
    tf = hashingTF.transform(documents )
    
    
    tfDistance =zip(Ids.collect(), tf.collect())
    def coSinDistance(vectorDistance):
        keys = set()
        keysTemp = set()
        vectorDict = {}
        for i in vectorDistance:
            for j in i:
                if type(j) != type('str') and type(j) != type(u'str'):
                    forlist = [list(  list(set(j.indices).union(set(a.indices))) for a in l if type(a) != type('str') and type(a) != type(u'str')) for l in vectorDistance]
                 
                    keysTemp = set()            
                    for f in forlist: 
                        k = set(f[0]) 
                        if not keysTemp:
                            keysTemp = k 
                        else:
                            keysTemp.union(k)
                        
                    jValues = j.values
                    jIndex = j.indices
                    for ind in range(len(jValues)):
                        if ind == 0:
                            vectorDict[i[0]] = {jIndex[ind]:jValues[ind]}
                        else: 
                            vectorDict[i[0]][jIndex[ind]] = jValues[ind]
        
    
            keys = keys.union(keysTemp)
    
        for key in keys:
            for K, V in vectorDict.items():
    
                if key not in V.keys():
                    V[key] = 0.0
    
                
        calcDict = {}
        for K,V in vectorDict.items():
            for k,v in V.items():
                if K in calcDict:
                    calcDict[K].append(v)
                else: 
                    calcDict[K] = [v]
        
        rankingDict ={}
        for K in calcDict:
            for k in calcDict:
                if K in rankingDict:
                    rankingDict[K][k] = spatial.distance.cosine(calcDict[K],calcDict[k])
                else:
                    rankingDict[K] ={k:spatial.distance.cosine(calcDict[K],calcDict[k])}
        
        rankingJson = []
        for K,V in rankingDict.items():
            for k,v in V.items():
                rankingJson.append(((K,k),v))
        
        ranking = sc.parallelize(rankingJson)
        return ranking
      
    
    tfRanking = coSinDistance(tfDistance)    
    tfRanking.map(lambda x: json.dumps(x)).saveAsTextFile("hdfs:///user/cdrayton/project/yelp_10k_outputs/month%s_yelp_10k_tfRANKING.txt"%month)
     
    for i in tfRanking.take(10):
        print i,"TF"

print "END_END_END\nEND_END_END\nEND_END_END" 


    
