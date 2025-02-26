# To pass in db password securely 
# can use below to spark-submit switch
```
--conf PROP=VALUE           Arbitrary Spark configuration property.
--properties-file FILE      Path to a file from which to load extra properties. If not
                            specified, this will look for conf/spark-defaults.conf.

```

## 1) --conf
```
spark-submit --conf spark.jdbc.password=mypassword --conf spark.jdbc.username=myusername
# to get the credential
val pw = spark.conf.get("spark.jdbc.password")
val user = spark.conf.get("spark.jdbc.username")
```
## 2) --properties-file
```
echo "spark.jdbc.password=mypassword" > credentials.properties
echo "spark.jdbc.username=myusername" >> credentials.properties

spark-submit --properties-file credentials.properties
```

## 3) encode the properties file
```
echo "spark.jdbc.password_b64encoded=$(echo -n mypassword | base64)" > credentials_b64.properties
spark-submit --properties-file credentials_b64.properties

import java.util.Base64 
import java.nio.charset.StandardCharsets 
val properties = new java.util.Properties()
properties.put("driver", "com.mysql.jdbc.Driver")
properties.put("url", "jdbc:mysql://mysql-host:3306")
properties.put("user", "myusername")
val password = new String(Base64.getDecoder().decode(spark.conf.get("spark.jdbc.password_b64encoded")), StandardCharsets.UTF_8)
properties.put("password", password)
val models = spark.read.jdbc(properties.get("url").toString, "ml_models", properties)

```
## 4) use enviroment variable

export myvariable=pass

```
scala> System.getenv("myvariable")
res0: String = pass


```
## 5) use config factory

```
// application.conf
my {
    secret {
        value = "super-secret"
        value = ${?VALUE}   
    }
}
-------------------------------------
import com.typesafe.config.ConfigFactory
import scala.util.Properties
 
class MyConfig(fileNameOption: Option[String] = None) {
     
  val config = fileNameOption.fold(
                  ifEmpty = ConfigFactory.load() )(
                  file => ConfigFactory.load(file) )
 
  def envOrElseConfig(name: String): String = {
    Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name)
    )
  }
}

val myConfig = new MyConfig()
val value = myConfig.envOrElseConfig("my.secret.value")
println(s"My secret value is $value")
```
