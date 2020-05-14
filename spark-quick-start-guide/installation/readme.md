### wget the spark tar file
```
wget http://apache.mirrors.hoobly.com/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop2.7.tgz
tar -zvxf spark-3.0.0-preview2-bin-hadoop2.7.tgz
```
### scp java jdk from your local to remote 
```
scp -i key-to-ec2.pem jdk-8u241-linux-x64.tar.gz ec2-user@35.172.128.36:~/
```
### Set up environment PATH,  or set up in .bash_profile file and do 'source .bash_profile' 
```
export SPARK_HOME=/home/ec2-user/spark-3.0.0-preview2-bin-hadoop2.7

export JAVA_HOME=/home/ec2-user/jdk1.8.0_241
export PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin
```
