package examples

import model.{MovieMap, RatingMap}
import org.apache.spark.{SparkConf, SparkContext}
import utils.LogManager

/**
  * Created by marcodoncel on 4/1/16.
  */
object BasicAnalyticsFromTextFileExample {

  def main(args: Array[String]): Unit = {
    //Spark Log is really verbose so the first thing is to set the log level to WARN
    LogManager.setStreamingLogLevelToWarn()
    val sparkConf = new SparkConf().setAppName("Movies analytics")
        .setMaster("local[2]")

    val sc = new SparkContext(sparkConf)

    //'''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    //record, directly caching the returned RDD will create many references to the same object.
    //If you plan to directly cache Hadoop writable objects, you should first copy them using a `map` function.

    //Processing from file in resources and converting to RDD[Rating] for easier management later on (Not neccesary)
    val ratingsRDD = sc.textFile(getClass().getResource("/u.data").getPath).flatMap(rawRating => RatingMap.fromSplittedString(rawRating.split('\t'))).cache()

    //Processing from file in resources and converting to RDD[Movie] for easier management later on (Not neccesary)
    val moviesRDD = sc.textFile(getClass().getResource("/u.item").getPath).flatMap(rawMovie => MovieMap.fromSplittedString(rawMovie.split('|')))


    //Now we'll calculate movies for what year are best voted
    //first of all we join both RDD (ratings and movies)

    //RDD[Int,Int] -> MovieId,Rating
    val movieRatingRDD = ratingsRDD.map(rating => (rating.movieId,rating.rating))
    val movieYearRDD = moviesRDD.map(movie => (movie.id,movie.release.getYear))

      // (K, V) join (K, W) --> (K, (V, W))
    val yearRatingJoinRDD = movieYearRDD.join(movieRatingRDD)
        .map(record => (record._2._1,(record._2._2,1)))
        .map(a=>a).reduceByKey((a,b) => (a._1+b._1,a._2+b._2))
        .map(year =>  (year._1,(math rint (year._2._1.toDouble/year._2._2) * 100) / 100) )



    val totalRatings = ratingsRDD.count
    val totalMovies = moviesRDD.count
    //Distribution of users that have voted each rating
    val ratingsCount = ratingsRDD.map(rating => (rating.rating,1)).reduceByKey(_ + _).collect()
    val yearRatingCount = yearRatingJoinRDD.collect()

    println("***************************************************************")
    println("******************   RATINGS ANALYTICS   **********************")
    println("***************************************************************")
    println("")
    println("")
    println("Number of ratings processed: "+totalRatings)
    println("Number of ratings processed: "+totalMovies)
    println("")
    println("")

    println("Rating distribution:")
    ratingsCount.sortBy(-_._1).foreach(rating => println(rating._1+" stars  -->  "+rating._2))
    println("")
    println("")

    println("Years most voted:")
    yearRatingCount.sortBy(-_._2).foreach(rating => println(rating._1+" (year)  -->  "+rating._2+" Stars"))

    sc.stop()
  }
}
