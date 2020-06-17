https://towardsdatascience.com/how-to-build-spark-from-source-and-deploy-it-to-a-kubernetes-cluster-in-60-minutes-225829b744f9
https://developer.sh/posts/spark-kubernetes-guide
```
docker build -t spark:latest -f kubernetes/dockerfiles/spark/Dockerfile .

docker image ls

kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role --clusterrole=edit  --serviceaccount=default:spark --namespace=default

bin/spark-submit \
--master k8s://https://kubernetes.docker.internal:6443 \
--deploy-mode cluster \
--name spark-pi \
--class org.apache.spark.examples.SparkPi \
--conf spark.executor.instances=2 \
--conf spark.kubernetes.container.image=spark:latest \
--conf spark.kubernetes.container.image.pullPolicy=Never \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
local:///opt/spark/examples/jars/spark-examples_2.12-3.1.0-SNAPSHOT.jar 10000000


kubectl get pods

#Use port forwarding to show Spark UI
kubectl port-forward <insert spark driver pod name> 4040:4040

#check log
kubectl -n=default logs -f <insert spark driver pod name>
```
