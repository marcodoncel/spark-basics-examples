package utils

import com.mongodb.casbah.commons.conversions.scala._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by marcodoncel on 3/30/16.
  */
object RDDUtils {

  def writeRDDToMongo[T:ClassTag](rdd: RDD[T], mongoUri: String)={

    RegisterJodaTimeConversionHelpers()
    val outputConfig = new Configuration()
    outputConfig.set("mongo.output.uri",
      mongoUri)

    rdd.map(element => (null,element)).saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], outputConfig)

  }


  // END OLD VISITORS
  def main(args: Array[String]) = {

  }

}
