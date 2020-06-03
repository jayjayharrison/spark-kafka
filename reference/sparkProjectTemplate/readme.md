### Simplified template - base off holdenk/sparkProjectTemplate.g8

```
git clone https://github.com/jayjayjohn/spark-kafka/reference/sparkProjectTemplate

cd sparkProjectTemplate

echo "test test world world count" > in.txt

sbt package

spark-submit --class com.example.sparkProject.CountingApp \
            --name myapp \
            --master local[1] \
            ./target/scala-2.12/spark-test-app_2.12-0.1.jar 
            in.txt out.txt

```
