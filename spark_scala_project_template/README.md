### Simplified template - base off holdenk/sparkProjectTemplate.g8

```
git clone https://github.com/jayjayjohn/sparkProject

cd sparkProject

echo "test test world world count" > in.txt

sbt package  ## package is a generate a light jar,  use sbt assembly if you want all library to be included in jar, you will need 

spark-submit --class com.example.sparkProject.CountingApp \
            --name myapp \
            --master local[1] \
            ./target/scala-2.12/spark_example_app_2.12-0.1.jar \
            in.txt out.txt

```
### sbt assembly might have merge error, add below to build.sbt
```
assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

```
