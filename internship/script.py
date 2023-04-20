from pyspark.sql import SparkSession
from os.path import abspath
import logging 
from pyspark.sql.functions import *
from pyspark.sql import functions as F
logging.basicConfig(level=logging.INFO)
warehouse_location = abspath('spark-warehouse') 




spark = SparkSession.builder.master("spark://localhost:7077").appName("demo").config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport().getOrCreate()




spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setLogLevel("INFO")
spark.sparkContext.setLogLevel("WARN")




def movies_data():
    logging.info("Reading data from movies and programatically specified the schema")
    movies_1 = spark.read.format("csv").option("inferSchema", True).option("header", False).option("delimiter", "::").load("hdfs:///data/movies.dat")
    movies_1.show(4)
    logging.info("renaming columns names as mentioned in the dataset documentation")
    global movies_2
    movies_2 =  movies_1.selectExpr('_c0 AS movieId','_c1 AS title','_c2 AS genres')
    movies_2.show(4)
    logging.info("Extracting the Year and Genre from the movies data")
    df_new = movies_2.withColumn('genres', F.split(movies_2.genres, '\|'))
    genre = df_new.select(col("title"), explode(col("genres")).alias("genres"))
    genre.show(10)
    df3 = movies_2.withColumn("year", movies_2.title.substr(-5,4))
    df3.show(5)
    #movies_2.createOrReplaceTempView('movies')
    logging.info("Saving movies Tables without defining DDL in Hive")
    movies_2.write.mode('overwrite').saveAsTable("movies")
    spark.sql("show tables in default").show()
    logging.info("Querying data from hive tables" )
    spark.sql("select * from movies limit 5").show()


def ratings_data():
    logging.info("Reading data from ratings and programatically specified the schema")
    ratings_1 = spark.read.format("csv").option("inferSchema", True).option("header", False).option("delimiter", "::").load("hdfs:///data/ratings.dat")
    ratings_1.show(4)
    logging.info("renaming columns names as mentioned in the dataset documentation")
    global ratings_2
    ratings_2 = ratings_1.selectExpr('_c0 AS UserIDs','_c1 AS MovieIDs','_c2 AS Ratings','_c3 AS  Timestamp')
    ratings_2.show(4)
    #ratings_2.createOrReplaceTempView('ratings') 
    logging.info("Saving ratings Tables without defining DDL in Hive")
    ratings_2.write.mode('overwrite').saveAsTable("ratings")
    spark.sql("show tables in default").show()
    logging.info("Querying data from hive tables" )
    spark.sql("select * from ratings limit 5").show()

def users_data():
    logging.info("Reading data from users and programatically specified the schema")
    users_1 = spark.read.format("csv").option("inferSchema", True).option("header", False).option("delimiter", "::").load("hdfs:///data/users.dat")
    users_1.show(4)
    logging.info("renaming columns names as mentioned in the dataset documentation")
    users_2 = users_1.selectExpr('_c0 AS UserID','_c1 AS Gender','_c2 AS Age','_c3 AS  Occupation','_c4 AS Zip_code' )
    users_2 = users_2.withColumn("Occupation",users_2["Occupation"].cast('string')) 
    global users_3 
    users_3 = users_2.withColumn("Occupation",when(col("Occupation") == 0, "other or not specified")
               .when(col("Occupation") == 1 , "academic/educator")
               .when(col("Occupation") == 2 , "artist")
               .when(col("Occupation") == 3 , "clerical/admin")
               .when(col("Occupation") == 4 , "college/grad student")
               .when(col("Occupation") == 5 , "customer service")
               .when(col("Occupation") == 6 , "doctor/health care")
               .when(col("Occupation") == 7 , "executive/managerial")
               .when(col("Occupation") == 8 , "farmer")
               .when(col("Occupation") == 9 , "homemaker")
               .when(col("Occupation") == 10 , "K-12 student")
               .when(col("Occupation") == 11 , "lawyer")
               .when(col("Occupation") == 12 , "programmer")
               .when(col("Occupation") == 13 , "retired")
               .when(col("Occupation") == 14 , "sales/marketing")
               .when(col("Occupation") == 15 , "scientist")
               .when(col("Occupation") == 16 , "self-employed")
               .when(col("Occupation") == 17 , "technician/engineer")
               .when(col("Occupation") == 18 , "tradesman/craftsman")
               .when(col("Occupation") == 19 , "unemployed")
               .otherwise("writer")) 
              
    users_3.show(4)
    #users_3.createOrReplaceTempView('users') 
    logging.info("Saving ratings Tables without defining DDL in Hive")
    users_3.write.mode('overwrite').saveAsTable("users")
    spark.sql("show tables in default").show()
    logging.info("Querying data from hive tables" )
    spark.sql("select * from users limit 5").show()
    

def broadcast_variable(): 
    users_data()
    ratings_data()
    movies_data()
    logging.info("broadcast variable example (i.e., joining  ratings and movies datasets )")
    global ratings_movie_join
    ratings_movie_join = ratings_2.join(broadcast(movies_2), ratings_2['MovieIDs']==movies_2['movieId'],'inner').drop(movies_2['movieId'])
    ratings_movie_join.show(3)



def Analytical_Queries():
    logging.info("#####Spark dataframe queries#######")
    broadcast_variable()
    logging.info("######Spark Analytical queries######")
    logging.info(" top 10 most viewed movies")
    top_view=ratings_movie_join.groupby("MovieIDs",'title').agg(count("*").alias("top_viewed_movies")).sort(desc("top_viewed_movies"))
    top_view.show(10)

    logging.info(" distinct list of genres available")
    df_new = movies_2.withColumn('genres', F.split(movies_2.genres, '\|'))
    distinct = df_new.select(col("title"), explode(col("genres")).alias("genres"))
    distinct.select('genres').distinct().show()
    
    logging.info("number of  movies for each genre")
    df_new = movies_2.withColumn('genres', F.split(movies_2.genres, '\|'))
    distinct = df_new.select(col("title"), explode(col("genres")).alias("genres"))
    distinct.groupBy('genres').count().orderBy('count', ascending=False).show()

    logging.info("number of movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)")
    print(' ')
    movies_strats_with_numbers=movies_2.filter(F.col("title").rlike("^[0-9]"))
    print('number of movies starting with numbers are: ',movies_strats_with_numbers.count())
    movies_strats_with_alphabats=movies_2.filter(~F.col("title").rlike("^[0-9]"))
    print('number of movies starting with alphabet are: ',movies_strats_with_alphabats.count())
    print(' ')

    logging.info("List of latest released movies")
    df3 = movies_2.withColumn("year", movies_2.title.substr(-5,4))
    df3.orderBy("year",ascending=False).show(10,truncate=False)

def Spark_SQL_Queries():
    Analytical_Queries()
    logging.info("#### Spark SQL Queries ###")
    logging.info("list of the oldest released movies")
    movies_2.createOrReplaceTempView("movies")
    sqlDF = spark.sql(" SELECT title,REGEXP_REPLACE(right(title,5) , '[^\\x20-\\x7E]', '') as year FROM movies order by year asc ")
    sqlDF.show()
    logging.info("number of  movies are released each year")
    sqlDF = spark.sql(" select count(p.title) as movies_per_year , p.year  from (SELECT *,REGEXP_REPLACE(right(title,5) , '[^\\x20-\\x7E]', '') as year FROM movies order by year asc) as p group by p.year order by movies_per_year desc ")
    sqlDF.show() 
    logging.info("number of movies are there for each rating")
    ratings_2.createOrReplaceTempView("ratings_2")
    sqlDF = spark.sql(" select count(MovieIDs) as number_of_movies , Ratings from ratings_2 group by Ratings ")
    sqlDF.show()
    logging.info("number of users have rated each movie")
    ratings_movie_join.createOrReplaceTempView("ratings")
    sqlDF = spark.sql(" select count(UserIDs) as number_of_users_rated , title from ratings group by title order by number_of_users_rated  desc")
    sqlDF.show(10,False)
    logging.info(" total ratings for each movie")
    #ratings_2.createOrReplaceTempView("ratings")
    sqlDF = spark.sql(" select sum(Ratings) as total_rating , title from ratings group by title order by total_rating  desc")
    sqlDF.show(10,False)
    logging.info(" average ratings for each movie")
    #ratings_2.createOrReplaceTempView("ratings")
    sqlDF = spark.sql(" select round(avg(Ratings),2) as average_rating , title from ratings group by title order by average_rating  desc")
    sqlDF.show(10,False)


if __name__ == '__main__':
    Spark_SQL_Queries()
    
  
     
