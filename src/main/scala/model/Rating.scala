package model

import com.mongodb.DBObject
import com.mongodb.casbah.commons.MongoDBObject
import org.bson.BSONObject
import org.bson.types.ObjectId
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

  def toBson(rating: Rating): DBObject ={
    MongoDBObject(
      "user_id"     -> rating.userId,
      "movie_id"     -> rating.movieId,
      "rating" -> rating.rating,
      "timestamp" -> rating.timestamp
    )
  }
  def fromBson(o: BSONObject): (ObjectId,Rating) = {
    (new ObjectId(o.get("_id").toString),Rating(
      userId = o.get("user_id").asInstanceOf[Integer],
      movieId = o.get("movie_id").asInstanceOf[Integer],
      rating = o.get("rating").asInstanceOf[Integer],
      timestamp = o.get("release").asInstanceOf[Long])
      )
  }
}
