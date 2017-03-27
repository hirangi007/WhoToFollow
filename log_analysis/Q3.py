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

filter_achille=text_file1.filter(lambda line: "Started Session " in line).map(lambda line: str(line.split()[-1])[:-1]).distinct()
filter_achille1=text_file2.filter(lambda line: "Started Session " in line).map(lambda line: str(line.split()[-1])[:-1]).distinct()

print "Q3: unique user names"
print "iliad : "+ str(filter_achille.collect())
print "odyssey : "+ str(filter_achille1.collect())
