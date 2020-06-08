# continously listen to file and push to kafka
### tail -f : -f watch file for change and print last 10 lines.  while loop is actually not necessary, put this in a bash.sh file
### tail -n1 -f ~/data/message.txt | while read LINE; do echo "$LINE"; done | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic message

```
# start zookeeper
nohup bin/zookeeper-server-start.sh config/zookeeper.properties > ~/nohup.out 2> ~/nohup.err < /dev/null &
# start kafka server
nohup bin/kafka-server-start.sh config/server.properties > ~/kafka_nohup.out 2> ~/kafka_nohup.err < /dev/null &
# create topic message 
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic message
# pass messgae EOF continously to topic
tail -n1 -f ~/data/message.txt | while read LINE; do echo "$LINE"; done | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic message
```

### bash script to append file 
#### nohup bash output_message.sh 10000 0.5 &>nohup.out &
```
# bash output_message.sh 100 0.1
# output_message.sh 
#!/bin/bash

cnt=$1
freq=$2

if [ -z "$freq" ]; then freq=1; fi
if [ -z "$cnt" ]; then cnt=20; fi

message=('hello' 'where are you from' 'im from iceland' 'oh, nice, how cold it there' 'no cold $

for x in $(seq 1 $cnt)
do
        TimeStamp=$(date '+%m-%d-%Y %HH"%MM"%S')
        echo "Message $x : ${message[$(( ${RANDOM} % ${#message[@]} ))]}   $TimeStamp" >> /home$

        sleep $freq
done

```
run
