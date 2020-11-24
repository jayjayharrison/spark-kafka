```
val detailsDf = Seq((123,"first123","xyz"))
            .toDF("userId","firstName","address")


val emailDf = Seq((123,"abc@gmail.com"),
              (123,"def@gmail.com"))
          .toDF("userId","email")


val foodDf = Seq((123,"food2",false,"Italian",2),
             (123,"food3",true,"American",3),
             (123,"food1",true,"Mediterranean",1))
        .toDF("userId","foodName","isFavFood","cuisine","score")


val gameDf = Seq((123,"chess",false,2),
             (123,"football",true,1))
         .toDF("userId","gameName","isOutdoor","score")

val emailGrp = emailDf.groupBy("userId").agg(collect_list("email").as("UserEmail"))

val foodGrp = foodDf
          .select($"userId", struct("score", "foodName","isFavFood","cuisine").as("UserFoodFavourites"))
          .groupBy("userId").agg(sort_array(collect_list("UserFoodFavourites")).as("UserFoodFavourites"))

val gameGrp = gameDf
          .select($"userId", struct("gameName","isOutdoor","score").as("UserGameFavourites"))
          .groupBy("userId").agg(collect_list("UserGameFavourites").as("UserGameFavourites"))

val result = detailsDf.join(emailGrp, Seq("userId"))
        .join(foodGrp, Seq("userId"))
        .join(gameGrp, Seq("userId"))

result.show(100, false)
```
