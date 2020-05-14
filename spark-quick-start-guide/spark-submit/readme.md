```
./bin/spark-submit \
  --class <main-class> \    \\name your main scala class ex: com.jay.test.myfirstclass
  --master <master-url> \    \\ url for cluster ex: local[2] (run on local host with 2 thread), yarn, spark://207.184.161.138:7077 if spark is in a standalone cluster
  --deploy-mode <deploy-mode> \   \\client, cluster
  --conf <key>=<value> \          \\ dynamically loading spark property. in key=value format. For values that contain spaces wrap “key=value” in quotes
  ... # other options             \\ ex:--conf spark.eventLog.enabled=false --config spark.app.name=jaysapp ,  if not specify, spark will refer to conf/spark-defaults.conf
  <application-jar> \
  [application-arguments]
```
