'''
Created on Mar 25, 2017

@author: hirangi
'''

from pyspark import SparkContext, SparkConf
from operator import add

conf = SparkConf().setAppName("Spark Count")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
text_file1 = sc.textFile("/home/hirangi/Downloads/iliad/")
text_file2 = sc.textFile("/home/hirangi/Downloads/odyssey/")

filter_achille=text_file1.filter(lambda line: "Started Session " in line).map(lambda line: (str(line.split()[-1])[:-1],1))
result = filter_achille.reduceByKey(add).collect()
filter_achille1=text_file2.filter(lambda line: "Started Session " in line).map(lambda line: (str(line.split()[-1])[:-1],1))
result1 = filter_achille.reduceByKey(add).collect()

print "Q4: sessions per user"
print "iliad : "+str(result)
print "odyssey : "+ str(result1)