package model

import utils.LogManager

/**
  * Created by marcodoncel on 4/1/16.
 **
  */

case class Rating (userId: Int,
                   movieId: Int,
                   rating: Int,
                   timestamp : Long) {

}

object RatingMap{
  val RATING_USER_ID_POSITION = 0
  val RATING_MOVIE_ID_POSITION = 1
  val RATING_RATING_POSITION = 2
  val RATING_TIMESTAMP_POSITION = 3

  def fromSplittedString(rawRating: Array[String]): Option[Rating] = {
    try {
      Some(Rating(
        userId = rawRating(RATING_USER_ID_POSITION).toInt,
        movieId = rawRating(RATING_MOVIE_ID_POSITION).toInt,
        rating = rawRating(RATING_RATING_POSITION).toInt,
        timestamp = rawRating(RATING_TIMESTAMP_POSITION).toLong
      ))
    } catch{
      case e:Exception => {
        LogManager.logWarnMessage("corrupted rating record: "+rawRating.mkString("|"))
        None
      }
    }
  }

}
