### Rdd.sortByKey()

### sortByKey sort the key in asc order by default 
### if you wish to sort by desc,  negate the key ie.  -key
```
ex sort k1 asc, k2 desc

((k1,-k2), k2 , v1, v2)
you need to store a k2 so you can map it back later

rdd.sortByKey().map( (_._1._1, _._2 ) )

```
