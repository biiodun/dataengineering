from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def parseInput(line):
    fields = line.split('|')
    return Row(user_id = int(fields[0]), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[4])

 
if __name__ == "__main__":
    #create a spark session
    spark = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host","127.0.0.1").getOrCreate()

    #Get raw textfile
    lines = spark.sparkContext.textFile("hdfs://127.0.0.1:9000/data/user-data.txt")

    #convert data to a RDD of Row objects with (UserID, age, gender, occupation, Zip)
    users = lines.map(parseInput)

    #convert usersRDD to a DataFrame
    usersDataset = spark.createDataFrame(users)

    #write dataframe data into cassandra
    usersDataset.write.format("org.apache.spark.sql.cassandra").mode("append").options(table="users", keyspace="movielens").save()

    #Read records from cassandra back into DataFrame
    readUsers = spark.read.format("org.apache.spark.sql.cassandra").options(table="users", keyspace="movielens").load()
    readUsers.createOrReplaceTempView("users")
    spark.sql("SELECT * from users where gender = 'F' ").show()
    spark.stop()

