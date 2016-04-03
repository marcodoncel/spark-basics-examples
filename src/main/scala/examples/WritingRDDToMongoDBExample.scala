package examples

import io.proximus.analytics.utils.Settings
import model.{MovieMap, RatingMap}
import org.apache.spark.{SparkConf, SparkContext}
import utils.{LogManager, RDDUtils}

/**
  * Created by marcodoncel on 4/1/16.
  */
object WritingRDDToMongoDBExample {

  def main(args: Array[String]): Unit = {

    val settings = Settings.loadSettings()

    //Spark Log is really verbose so the first thing is to set the log level to WARN
    LogManager.setStreamingLogLevelToWarn()
    val sparkConf = new SparkConf().setAppName(settings.sparkAppName)
        .setMaster(settings.sparkMaster)

    val sc = new SparkContext(sparkConf)

    //'''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    //record, directly caching the returned RDD will create many references to the same object.
    //If you plan to directly cache Hadoop writable objects, you should first copy them using a `map` function.

    //Processing from file in resources and converting to RDD[Rating] for easier management later on (Not neccesary)
    val ratingsRDD = sc.textFile(getClass().getResource("/u.data").getPath).flatMap(rawRating => RatingMap.fromSplittedString(rawRating.split('\t'))).cache()

    //Processing from file in resources and converting to RDD[Movie] for easier management later on (Not neccesary)
    val moviesRDD = sc.textFile(getClass().getResource("/u.item").getPath).flatMap(rawMovie => MovieMap.fromSplittedString(rawMovie.split('|'))).cache()


    println("Loading "+moviesRDD.count+" movies to MongoDB...")
    RDDUtils.writeRDDToMongo(moviesRDD.map(MovieMap.toBson), settings.mongoMoviesUri)
    println("Loading "+ratingsRDD.count+" ratings to MongoDB...")
    RDDUtils.writeRDDToMongo(ratingsRDD.map(RatingMap.toBson), settings.mongoRatingsUri)

    sc.stop()
  }
}
