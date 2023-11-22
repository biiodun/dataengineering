from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open ("/Users/abiodunakinlawon/Documents/sparkSQL_test/data2.txt") as f:
    
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames
    

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

 
if __name__ == "__main__":
    #create spark session
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    #load our movie ID:Name Dictionary
    movieNames = loadMovieNames()

    #Get the raw Data
    lines = spark.sparkContext.textFile("data.txt")

    #convert it to a RDD of Row objects with (movieId, rating)
    movies = lines.map(parseInput)

    #convert it to a DataFrame
    movieDataSet = spark.createDataFrame(movies)

    #compute average rating for each movieId
    averageRatings = movieDataSet.groupBy("movieID").avg("rating")

    #compute average rating for each movieId
    counts = movieDataSet.groupBy("movieID").count()

    #Join the two together ( we now have movieID, avg(ratings), and count coluomns)
    averageAndCounts = counts.join(averageRatings, "movieID")

    pop_average = averageAndCounts.filter("count > 10")
    
    #pull the top 10 results
    topTen = pop_average.orderBy("avg(rating)").take(10)

    #print them out and convert movieID's to names as we go
    for movie in topTen:
        print (movieNames[movie[0]], movie[1], movie[2])

    spark.stop()
    
    