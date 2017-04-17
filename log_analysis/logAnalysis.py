'''
Created on Apr 15, 2017

@author: hirangi
'''

from operator import add
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import sys


conf = SparkConf().setAppName("log_analysis").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

question_num=sys.argv[1]
input1=sys.argv[2]
input2=sys.argv[3]


def ques_1(input1,input2):
    file1=sc.textFile(input1)
    file2=sc.textFile(input2)
    counts=file1.count()
    counts2=file2.count()
    print "Q1: line counts"
    print "iliad : "+ str(counts)
    print "odyssey : " + str(counts2)
 
def ques_2(input1,input2):

    file1=sc.textFile(input1)
    file2=sc.textFile(input2)
    filter_achille=file1.filter(lambda line: "Starting Session " in line).filter(lambda line: "of user achille." in line)
    filter_achille2=file2.filter(lambda line: "Starting Session " in line).filter(lambda line: "of user achille." in line)

    print "Q2: sessions of user achille"
    print "iliad : "+ str(filter_achille.count())
    print "odyssey : "+ str(filter_achille2.count())

def q3(input1,input2):
    text_file1=sc.textFile(input1)
    text_file2=sc.textFile(input2)
    filter_achille=text_file1.filter(lambda line: "Started Session " in line).map(lambda line: str(line.split()[-1])[:-1]).distinct()
    filter_achille1=text_file2.filter(lambda line: "Started Session " in line).map(lambda line: str(line.split()[-1])[:-1]).distinct()

    print "Q3: unique user names"
    print "iliad : "+ str(filter_achille.collect())
    print "odyssey : "+ str(filter_achille1.collect())

def ques_4(input1,input2):
    text_file1=sc.textFile(input1)
    text_file2=sc.textFile(input2)
    filter_achille=text_file1.filter(lambda line: "Started Session " in line).map(lambda line: (str(line.split()[-1])[:-1],1))
    result = filter_achille.reduceByKey(add).collect()
    filter_achille1=text_file2.filter(lambda line: "Started Session " in line).map(lambda line: (str(line.split()[-1])[:-1],1))
    result1 = filter_achille1.reduceByKey(add).collect()
    print "Q4: sessions per user"
    print "iliad : "+str(result)
    print "odyssey : "+ str(result1)

def ques_5(input1,input2):
    text_file1=sc.textFile(input1)
    text_file2=sc.textFile(input2)
    filter_achille=text_file1.filter(lambda line: "error" in line.lower())
    filter_achille1=text_file2.filter(lambda line: "error" in line.lower())

    print "Q5: number of errors"
    print "iliad : "+ str(filter_achille.count())
    print "odyssey : "+ str(filter_achille1.count())

def ques_6(input1,input2):
    text_file1=sc.textFile(input1)
    text_file2=sc.textFile(input2)
    filter_achille=text_file1.filter(lambda line: "error" in line.lower()).map(lambda line: (str(" ".join(line.split()[3:])),1))
    result = filter_achille.reduceByKey(add).map(lambda (key,value): (value,key)).sortByKey(ascending=False).take(5)

    filter_achille1=text_file2.filter(lambda line: "error" in line.lower()).map(lambda line: (str(" ".join(line.split()[3:])),1))
    result1 = filter_achille1.reduceByKey(add).map(lambda (key,value): (value,key)).sortByKey(ascending=False).take(5)

    print "Q6: 5 most frequent error messages"
    print "iliad : \n "+str(result)
    print "odyssey : \n "+ str(result1)
        
def ques_7(input1,input2):

    text_file1=sc.textFile(input1)
    text_file2=sc.textFile(input2)
    filter_achille=text_file1.filter(lambda line: "Started Session " in line).map(lambda line: (str(line.split()[-1])[:-1],line.split()[3])).groupByKey().mapValues(list)\
    .map(lambda (key,value) : (key,str(list(set(value))[0])))
    
    filter_achille1=text_file2.filter(lambda line: "Started Session " in line).map(lambda line: (str(line.split()[-1])[:-1],line.split()[3])).groupByKey().mapValues(list)\
    .map(lambda (key,value) : (key,str(list(set(value))[0])))
    
    
    finalResult = filter_achille.join(filter_achille1)
    
    print finalResult.map(lambda (key,value) : key).collect() 
      

def ques_8(input1,input2):
    text_file1=sc.textFile(input1)
    text_file2=sc.textFile(input2)
    filter_achille=text_file1.filter(lambda line: "Started Session " in line).map(lambda line: (str(line.split()[-1])[:-1],line.split()[3])).groupByKey().mapValues(list)\
    .map(lambda (key,value) : (key,str(list(set(value))[0])))
    
    filter_achille1=text_file2.filter(lambda line: "Started Session " in line).map(lambda line: (str(line.split()[-1])[:-1],line.split()[3])).groupByKey().mapValues(list)\
    .map(lambda (key,value) : (key,str(list(set(value))[0])))
    
    
    finalResult = filter_achille.join(filter_achille1)
    firstHost = filter_achille.subtractByKey(finalResult)
    secondHost = filter_achille1.subtractByKey(finalResult)
    
    print firstHost.collect() + secondHost.collect() 
    
if(question_num=='1'):
    ques_1(input1, input2)    

if(question_num=='2'):
    ques_2(input1,input2)

if(question_num=='3'):
    q3(input1,input2)
    
if(question_num=='4'):
    ques_4(input1,input2)

if(question_num=='5'):
    ques_5(input1,input2)

if(question_num=='6'):
    ques_6(input1,input2)

if(question_num=='7'):
    ques_7(input1,input2)

if(question_num=='8'):
    ques_8(input1,input2)
    