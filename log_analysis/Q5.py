'''
Created on Mar 27, 2017

@author: hirangi
'''
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Spark Count")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
text_file1 = sc.textFile("/home/hirangi/Downloads/iliad/")
text_file2 = sc.textFile("/home/hirangi/Downloads/odyssey/")

filter_achille=text_file1.filter(lambda line: "error" in line.lower())
filter_achille1=text_file2.filter(lambda line: "error" in line.lower())

print "Q5: number of errors"
print "iliad : "+ str(filter_achille.count())
print "odyssey : "+ str(filter_achille1.count())
