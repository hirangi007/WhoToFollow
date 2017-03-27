'''
Created on Mar 25, 2017

@author: hirangi
'''

from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName("Spark Count")
sc = SparkContext(conf=conf)
text_file1 = sc.textFile("/home/hirangi/Downloads/odyssey/")
text_file2 = sc.textFile("/home/hirangi/Downloads/iliad/")

lines1=text_file1.count()
lines2=text_file2.count()

print "Q1: line counts"
print "iliad : "+ str(lines2)
print "odyssey : " + str(lines1)
