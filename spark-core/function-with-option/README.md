

```
  def parseNames(line: String) : Option[(Int, String)] = {
   
    var fields = line.split('\"')
    
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None /
    }
  }

// flat Map will remove None
val namesRdd = names.flatMap(parseNames)

// lookup function in pairedRDD
namesRdd.lookup(mostPopular._2)(0)


// count length of Value
def countCoOccurrences(line: String) = {
    var elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )}

```
