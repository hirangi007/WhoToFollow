'''
Created on Mar 24, 2017

@author: hirangi
'''




from pyspark.sql import Row, SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating





def assignId(bookData):
    global i
    i = i + 1
    return (bookData[0],(i,bookData[1]))

def ratingsRows((isbn,value)):
    (ratingDetails,bookDetails) = value
    (userID,ratingValue) = ratingDetails
    (bookIds,bookTitle) = bookDetails
    return Rating(int(userID), int(bookIds),float(ratingValue)/2)

conf = SparkConf().setMaster("local[*]").setAppName("BOOK_RECOMMENDATION")

sc = SparkContext(conf=conf)
spark = SparkSession \
    .builder \
    .appName("Collaborative Filtering") \
    .getOrCreate()



data = sc.textFile("file:///home/hirangi/workspace/Test/ratings3.csv")
books= sc.textFile("file:///home/hirangi/workspace/Test/books1.csv")
i = 0

booksRdd = books.map(lambda l: l.split(",")).map(assignId)
parts = data.map(lambda row: row.split(",")).map(lambda l: (l[1],(l[0],l[2])))
ratings = parts.join(booksRdd)
ratingsData = ratings.map(ratingsRows)

# Build the recommendation model using Alternating Least Squares
rank = 10
numIterations = 10
model = ALS.train(ratingsData, rank, numIterations)

firstResult = model.recommendProducts(383,5)
print " user: 383"
print "======================"
print " Top 5 Recommendation"
print "======================"
for temp in firstResult:
    print temp.product
print "======================"

firstResult = model.recommendProducts(276729,5)
print " user: 276729"
print "======================"
print " Top 5 Recommendation"
print "======================"
for temp in firstResult:
    print temp.product
print "======================"


firstResult = model.recommendProducts(472,5)
print " user: 472"
print "======================"
print " Top 5 Recommendation"
print "======================"
for temp in firstResult:
    print temp.product
print "======================"


firstResult = model.recommendProducts(43397,5)
print " user: 43397"
print "======================"
print " Top 5 Recommendation"
print "======================"
for temp in firstResult:
    print temp.product
print "======================"

# Evaluate the model on training data
testdata = ratingsData.map(lambda p: (p[0], p[1]))
predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
ratesAndPreds = ratingsData.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))
