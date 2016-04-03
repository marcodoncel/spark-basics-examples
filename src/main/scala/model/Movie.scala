package model

import com.mongodb.DBObject
import com.mongodb.casbah.commons.MongoDBObject
import org.bson.BSONObject
import org.bson.types.ObjectId
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import utils.LogManager;

/**
  * Created by marcodoncel on 4/1/16.
  */

case class Movie (id: Int,
                  title: String,
                  release: DateTime,
                  videoRelease : DateTime,
                  imdbURL: String,
                  genres: List[String]) {

}

object MovieMap{
  val MOVIE_ID_POSITION = 0
  val MOVIE_TITLE_POSITION = 1
  val MOVIE_RELEASE_POSITION = 2
  val MOVIE_VIDEO_RELEASE_POSITION = 3
  val MOVIE_IMDB_POSITION = 4


  val GENRES_OFFSET = 5

  val GENRES_LIST: Array[String] = Array("unknown","Action","Adventure","Animation","Children","Comedy","Crime","Documentary","Drama","Fantasy",
  "Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thrille","War","Western")
  /**
  Information about the items (movies); this is a tab separated
              list of
              movie id | movie title | release date | video release date |
              IMDb URL | unknown | Action | Adventure | Animation |
              Children's | Comedy | Crime | Documentary | Drama | Fantasy |
              Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
              Thriller | War | Western |
              The last 19 fields are the genres, a 1 indicates the movie
              is of that genre, a 0 indicates it is not; movies can be in
              several genres at once.
              The movie ids are the ones used in the u.data data set.
    */

  def fromSplittedString(rawMovie: Array[String]): Option[Movie] = {
    try{
      Some(Movie(
        id = rawMovie(MOVIE_ID_POSITION).toInt,
        title = rawMovie(MOVIE_TITLE_POSITION),
        release = DateTimeFormat.forPattern("dd-MMM-yyyy").parseDateTime(rawMovie(MOVIE_RELEASE_POSITION)),
        videoRelease = DateTimeFormat.forPattern("dd-MMM-yyyy").parseDateTime(rawMovie(MOVIE_RELEASE_POSITION)),
        imdbURL = rawMovie(MOVIE_IMDB_POSITION),
        genres = (GENRES_OFFSET to 23).flatMap(index => if(rawMovie(index).toInt==1) Some(GENRES_LIST(index - GENRES_OFFSET)) else None).toList
      ))
    } catch{
      case e:Exception => {
        LogManager.logWarnMessage("corrupted movie record: "+rawMovie.mkString("|"))
        None
      }
    }
  }

  def toBson(movie: Movie): DBObject ={
    MongoDBObject(
      "movie_id"     -> movie.id,
      "title"     -> movie.title,
      "release" -> movie.release,
      "video_release" -> movie.videoRelease,
      "imdb_url" -> movie.imdbURL,
      "genres" -> movie.genres
    )
  }
  def fromBson(o: BSONObject): (ObjectId,Movie) = {
    (new ObjectId(o.get("_id").toString),Movie(
      id = o.get("movie_id").asInstanceOf[Integer],
      title = o.get("title").toString,
      release = o.get("release").asInstanceOf[DateTime],
      videoRelease = o.get("video_release").asInstanceOf[DateTime],
      imdbURL = o.get("imdb_url").asInstanceOf[String],
      genres = o.get("genres").asInstanceOf[List[String]])
      )
  }
}
