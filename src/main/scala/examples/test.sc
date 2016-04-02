import model.{MovieMap, RatingMap}

val rawRating = "286\t1014\t5\t879781125"

RatingMap.fromSplittedString(rawRating.split('\t'))

val rawMovie = List("1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0")

rawMovie.flatMap(record => MovieMap.fromSplittedString2(record.split('|')))

rawMovie(0).split('|').mkString("|")