from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("PopularMovie")
sc = SparkContext(conf=conf)

lines = sc.textFile("/user/ncn251/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
print("movies=======", movies)
movieCounts = movies.reduceByKey(lambda x, y: x+y)

flipped = movieCounts.map(lambda x, y: (y, x))
sortedMovies = flipped.sortByKey()
results = sortedMovies.collect()

for result in results:
    print(result)
