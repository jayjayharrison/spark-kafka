```
import java.nio.charset.CodingErrorAction
import scala.io.Codec

var nameDict = sc.broadcast(loadMovieNames)


val sortedMoviesWithNames = sortedMovies.map( x  => (nameDict.value.getOrElse(x._2,null), x._1) )


def loadMovieNames() : Map[Int, String] = {
    // Handle character encoding issues:
    implicit val codec = Codec("ISO-8859-1") // This is the current encoding of u.item, not UTF-8.

    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
     return movieNames
  }

```
