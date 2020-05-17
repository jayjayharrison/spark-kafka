```
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j._


Logger.getLogger("org").setLevel(Level.ERROR)

//Create Streaming Context
val ssc = new StreamingContext(sc, Seconds(1))

//Create Stream on Directory
val lines = ssc.socketTextStream("localhost",8888)

val error = lines.filter(_.contains("error"))

error.print()

//Start Computation
ssc.start()
ssc.awaitTermination()

```
