package io.proximus.analytics.utils

import com.typesafe.config.{ConfigFactory, Config}

/**
  * Created by marcodoncel on 2/19/15.
  */
case class Settings(config: Config) extends Serializable {
  //MongoDb parameters
  val mongoMoviesUri = config getString "examples.connectionStrings.mongodb.uri.movies"
  val mongoRatingsUri = config getString "examples.connectionStrings.mongodb.uri.ratings"

  //Spark parameters
  val sparkAppName = config getString "examples.connectionStrings.spark.appname"
  val sparkMaster = config getString "examples.connectionStrings.spark.master"
}

object Settings {

  def loadSettings(): Settings = {

    val config = ConfigFactory.load("configuration")

    new Settings(config)
  }

  /** Prints the System.getProperties key: value pairs one per line.
    */
  def printSystemProperties() {

    val p = System.getProperties
    val keys = p.keys

    while (keys.hasMoreElements) {
      val k = keys.nextElement
      val v = p.get(k)
      println(k + ": " + v)
    }
  }
}
