'''
Created on Mar 27, 2017

@author: hirangi
'''
from pyspark import SparkContext, SparkConf
from operator import add

conf = SparkConf().setAppName("Spark Count")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
text_file1 = sc.textFile("/home/hirangi/Downloads/iliad/")
text_file2 = sc.textFile("/home/hirangi/Downloads/odyssey/")

filter_achille=text_file1.filter(lambda line: "error" in line.lower()).map(lambda line: (str(" ".join(line.split()[3:])),1))
result = filter_achille.reduceByKey(add).map(lambda (key,value): (value,key)).sortByKey(ascending=False).take(5)

filter_achille1=text_file2.filter(lambda line: "error" in line.lower()).map(lambda line: (str(" ".join(line.split()[3:])),1))
result1 = filter_achille1.reduceByKey(add).map(lambda (key,value): (value,key)).sortByKey(ascending=False).take(5)


print "Q6: 5 most frequent error messages"
print "iliad : \n "+str(result)
print "odyssey : \n "+ str(result1)