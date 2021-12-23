// struct, collect_list

val detailsDf = Seq((123, "first123", "xyz")).toDF("userId", "firstName", "address")

val emailDf = Seq((123,"abc@gmail.com"),(123,"def@gmail.com")).toDF("userId","email")

val foodDf = Seq((123,"food2",false,"Italian",2), (123,"food3",true,"American",3), (123,"food1",true,"Mediterranean",1)).toDF("userId","foodName","isFavFood","cuisine","score")

val emailGrp = emailDf.groupBy("userId").agg(collect_list("email").as("UserEmail"))
emailGrp.show(false)
/*
+------+------------------------------+
|userId|UserEmail                     |
+------+------------------------------+
|123   |[abc@gmail.com, def@gmail.com]|
+------+------------------------------+
*/
val foodGrp = foodDf.
            select($"userId", struct("score", "foodName","isFavFood","cuisine").as("UserFoodFavourites")).
            groupBy("userId").agg(sort_array(collect_list("UserFoodFavourites")).as("UserFoodFavourites"))
foodGrp.show(false)
/*
+------+-----------------------------------------------------------------------------------------+
|userId|UserFoodFavourites                                                                       |
+------+-----------------------------------------------------------------------------------------+
|123   |[[1, food1, true, Mediterranean], [2, food2, false, Italian], [3, food3, true, American]]|
+------+-----------------------------------------------------------------------------------------+
*/

val result = detailsDf.join(emailGrp, Seq("userId")).join(foodGrp, Seq("userId"))
result.show(false)    
+------+---------+-------+------------------------------+-----------------------------------------------------------------------------------------+
|userId|firstName|address|UserEmail                     |UserFoodFavourites                                                                       |
+------+---------+-------+------------------------------+-----------------------------------------------------------------------------------------+
|123   |first123 |xyz    |[abc@gmail.com, def@gmail.com]|[[1, food1, true, Mediterranean], [2, food2, false, Italian], [3, food3, true, American]]|
+------+---------+-------+------------------------------+-----------------------------------------------------------------------------------------+
```
