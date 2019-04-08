from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("validate JSON")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName(
    "validate JSON").config(conf=conf).getOrCreate()

jsonFile = spark.read.option("multiline", "true").json(
    '/user/ncn251/sample.json')

state = jsonFile.select('state').collect()[0].state
categories = jsonFile.select('categories').collect()[0].categories
hasBusinessGarage = jsonFile.select('attributes').collect()[
    0].attributes.BusinessParking.garage

if state and 'Nightlife' in categories and hasBusinessGarage:
    print("Business is in Arizona and is a nightlife and has a business garage")
else:
    print("All the conditions are not fulfilled")
