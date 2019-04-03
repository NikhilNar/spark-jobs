from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)
lines = sc.textFile("/user/ncn251/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print(key, value)
