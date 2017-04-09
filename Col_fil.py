'''
Created on Mar 24, 2017

@author: hirangi
'''

from pyspark.sql import Row, SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator


def assignId(bookData):
    global i
    i = i + 1
    return (bookData[0],(i,bookData[1]))

def ratingsRows((isbn,value)):
    (ratingDetails,bookDetails) = value
    (userID,ratingValue) = ratingDetails
    (bookIds,bookTitle) = bookDetails
    return Row(userId=int(userID), bookId=int(bookIds),rating=float(ratingValue), title=bookTitle)

conf = SparkConf().setMaster("local[*]").setAppName("BOOK_RECOMMENDATION")

sc = SparkContext(conf=conf)
spark = SparkSession \
    .builder \
    .appName("Collaborative Filtering") \
    .getOrCreate()

data = sc.textFile("file:///home/hirangi/workspace/Test/ratings4.csv")
books= sc.textFile("file:///home/hirangi/workspace/Test/books1.csv")
i = 0
booksRdd = books.map(lambda l: l.split(",")).map(assignId)
parts = data.map(lambda row: row.split(",")).map(lambda l: (l[1],(l[0],l[2])))
ratings = parts.join(booksRdd)
ratingsData = ratings.map(ratingsRows)
ratings = spark.createDataFrame(ratingsData)
(training, test) = ratings.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="bookId", ratingCol="rating")
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
print model.itemFactors.collect()
predictions = model.transform(test)

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))


