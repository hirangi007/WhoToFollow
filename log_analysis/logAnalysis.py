'''
Created on Apr 15, 2017

@author: hirangi
'''

import sys
import glob
from pyspark import SparkContext, SparkConf
import re
from operator import add
from pyspark.sql import SQLContext
from collections import Counter

from pyspark.sql import Row 
conf = SparkConf().setAppName("wordcount1").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

question_num=sys.argv[1]
input=sys.argv[2]
input2=sys.argv[3]


def ques_1(input,input2):
    file=sc.textFile(input)
    file2=sc.textFile(input2)
    counts=file.count()
    counts2=file2.count()
    print "Q1: line counts"
    print "iliad : "+ str(counts)
    print "odyssey : " + str(counts2)
 
def ques_2(input,input2):

    file=sc.textFile(input)
    file2=sc.textFile(input2)
    filter_achille=file.filter(lambda line: "Starting Session " in line).filter(lambda line: "of user achille." in line)
    filter_achille2=file2.filter(lambda line: "Starting Session " in line).filter(lambda line: "of user achille." in line)

    print "Q2: sessions of user achille"
    print "iliad : "+ str(filter_achille.count())
    print "odyssey : "+ str(filter_achille2.count())

def ques_3(input,input2): 
    file=sc.textFile(input)
    file2=sc.textFile(input2)
    linesStarting1 = file.filter(lambda s: 'Starting Session' in s)
    AllUsers1=linesStarting1.filter(lambda s: ' of user ' in s)
    AllUsers11=AllUsers1.filter(lambda s: '.' in s)
    #print(AllUsers11.count())
 
    linesStarting2 = file2.filter(lambda s: 'Starting Session' in s)
    AllUsers2=linesStarting2.filter(lambda s: ' of user ' in s)
    AllUsers22=AllUsers2.filter(lambda s: '.' in s)
    #print(AllUsers22.count())
 
    csv_data1 = AllUsers11.map(lambda l: l.split(" "))
    row_data1 = csv_data1.map(lambda p: Row(
    host=(p[3]),
    user=(p[10]),
    user_id=(p[7])
        )
    
    )
    csv_data2 = AllUsers22.map(lambda l: l.split(" "))
    row_data2 = csv_data2.map(lambda p: Row(
    host=(p[3]),
    user=(p[10]),
    user_id2=(p[7])
    
        )
    )
    interactions_df = sqlContext.createDataFrame(row_data1)
    interactions_df.registerTempTable("interactions") 
    #print('hey') 
    content_size_stats = (sqlContext.sql("select distinct user from interactions where user !='user'" ))
    interactions2_df = sqlContext.createDataFrame(row_data2)
    interactions2_df.registerTempTable("interactions2") 
    #print('hey') 
    content_size_stats2 = (sqlContext.sql("select distinct user from interactions2 where user !='user'" ))
 
    #print(content_size_stats.show())
    return interactions_df, interactions2_df

def q3(input,input2):
    text_file1=sc.textFile(input)
    text_file2=sc.textFile(input2)
    filter_achille=text_file1.filter(lambda line: "Started Session " in line).map(lambda line: str(line.split()[-1])[:-1]).distinct()
    filter_achille1=text_file2.filter(lambda line: "Started Session " in line).map(lambda line: str(line.split()[-1])[:-1]).distinct()

    print "Q3: unique user names"
    print "iliad : "+ str(filter_achille.collect())
    print "odyssey : "+ str(filter_achille1.collect())

def ques_4(input,input2):
    text_file1=sc.textFile(input)
    text_file2=sc.textFile(input2)
    filter_achille=text_file1.filter(lambda line: "Started Session " in line).map(lambda line: (str(line.split()[-1])[:-1],1))
    result = filter_achille.reduceByKey(add).collect()
    filter_achille1=text_file2.filter(lambda line: "Started Session " in line).map(lambda line: (str(line.split()[-1])[:-1],1))
    result1 = filter_achille1.reduceByKey(add).collect()

    print "Q4: sessions per user"
    print "iliad : "+str(result)
    print "odyssey : "+ str(result1)

def ques_5(input,input2):
    text_file1=sc.textFile(input)
    text_file2=sc.textFile(input2)
    filter_achille=text_file1.filter(lambda line: "error" in line.lower())
    filter_achille1=text_file2.filter(lambda line: "error" in line.lower())

    print "Q5: number of errors"
    print "iliad : "+ str(filter_achille.count())
    print "odyssey : "+ str(filter_achille1.count())


def ques_6(input,input2):
    text_file1=sc.textFile(input)
    text_file2=sc.textFile(input2)
    filter_achille=text_file1.filter(lambda line: "error" in line.lower()).map(lambda line: (str(" ".join(line.split()[3:])),1))
    result = filter_achille.reduceByKey(add).map(lambda (key,value): (value,key)).sortByKey(ascending=False).take(5)

    filter_achille1=text_file2.filter(lambda line: "error" in line.lower()).map(lambda line: (str(" ".join(line.split()[3:])),1))
    result1 = filter_achille1.reduceByKey(add).map(lambda (key,value): (value,key)).sortByKey(ascending=False).take(5)

    print "Q6: 5 most frequent error messages"
    print "iliad : \n "+str(result)
    print "odyssey : \n "+ str(result1)
        
def ques_7(input,input2):

    table1_df, table2_df = ques_3(input,input2)
    table1_df.registerTempTable("table1") 
    table2_df.registerTempTable("table2") 
    queryResult=(sqlContext
                      .sql("select distinct table1.user\
        from table1 inner join table2\
        on table1.user = table2.user\
        where table1.user != 'user' " )).show()  

def ques_8(input,input2):
    table1_df, table2_df = ques_3(input,input2)
    table1_df.registerTempTable("table1")
    table2_df.registerTempTable("table2") 
    queryResult=(sqlContext
                      .sql("select distinct table1.user,table1.host\
                            from table1 union select distinct table2.user,table2.host where not exists (select distinct table1.user\
                            from table1 inner join table2\
                            on table1.user = table2.user\
                            where table1.user != 'user' )")).show() 
                    
if(question_num=='1'):
    ques_1(input, input2)    

if(question_num=='2'):
    ques_2(input,input2)

if(question_num=='3'):
    q3(input,input2)
    
if(question_num=='4'):
    ques_4(input,input2)

if(question_num=='5'):
    ques_5(input,input2)

if(question_num=='6'):
    ques_6(input,input2)

if(question_num=='7'):
    ques_7(input,input2)

if(question_num=='8'):
    ques_8(input,input2)
    