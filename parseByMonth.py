# -*- coding: utf-8 -*-
"""
Created on Sun Jan 25 14:12:35 2015

@author: Colin
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Jan 23 15:34:07 2015
@author: Colin
"""

##########################################
#### Regular Expression Tutorial #########
##########################################
import json
import pandas as pd
from glob import glob
import re
import cPickle as pickle
# Open the file (make sure its in the same directory as this file)
f = open('yelp_dataset_challenge_academic_dataset.json')#('yelptest.json')
s = f.read()
f.close()
s = s.split('\n')
print len(s)
""" First I need to seperate Businesses, Reviews and User Info into seperate lists."""

i = 0

for r in s:
    try:
        r = json.loads(r)
        if r['type'] == 'review':
            date = r['date']
            year =  (int(str(date.split('-')[0]))-2004)*12
            month =  int(str(date.split('-')[1])) 
            months = month + year
            with open("yelp_FULL_months/month%s_yelp_FULL.txt"%months,'a') as myfile:
                json.dump(r,myfile)
                myfile.write('\n')
    except ValueError:
        print s.index(r),"  ",i,"  ",r

    i +=1















    