'''
Created on Mar 25, 2017

@author: hirangi
'''

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Spark Count")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
text_file1 = sc.textFile("/home/hirangi/Downloads/iliad/")
text_file2 = sc.textFile("/home/hirangi/Downloads/odyssey/")

filter_achille=text_file1.filter(lambda line: "Starting Session " in line).filter(lambda line: "of user achille." in line)
filter_achille2=text_file2.filter(lambda line: "Starting Session " in line).filter(lambda line: "of user achille." in line)

print "Q2: sessions of user achille"
print "iliad : "+ str(filter_achille.count())
print "odyssey : "+ str(filter_achille2.count())
