```
Go to https://kafka.apache.org/downloads

Click on the link and go to mirror for kafka_2.11-1.0.0.tgz. 

cd /opt/
sudo wget http://apache.mirrors.hoobly.com/kafka/2.5.0/kafka_2.12-2.5.0.tgz

sudo tar xzf kafka_2.12-2.5.0.tgz

##create symbolic name
sudo ln -s /opt/kafka_2.12-2.5.0 /opt/kafka

export PATH=$PATH:/opt/kafka/bin

Starting zookeeper-server: zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
Starting kafka-server: kafka-server-start.sh -daemon "/opt/kafka/config/server.properties"
Use -daemon to submit zookeeper and kafka servers in background
```
