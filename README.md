### Introduction

This project is made for module CS5424 Distributed Databases from National University of Singapore.

The aim for this project is to acquire practical experience with using distributed database systems  for application development.

Language used: [Java](http://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html)

### Prerequisite

- Download and install [Java 8](http://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html)
- Download and install [Apache Cassandra 3.11.0](http://www.apache.org/dyn/closer.lua/cassandra/3.11.0/apache-cassandra-3.11.0-bin.tar.gz)

### Setup Environment

1. Set environment variables
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.222.b10-1.el7_7.x86_64
export PATH=$JAVA_HOME/bin:$PATH
export CASSANDRA_HOME=/temp/Cassandra
export PATH=$CASSANDRA_HOME/bin:$PATH
export PATH=/home/stuproj/cs4224m/apache-maven-3.6.2/bin:$PATH

2. Configure all nodes in cassandra - this has been done in the 5 machines
Configure cassandra nodes in a cluster using the link 
https://docs.datastax.com/en/cassandra/3.0/cassandra/initialize/initializeSingleDS.html

3. Start cassandra on local machine: 
cd /temp/Cassandra

4. Copy the jar files into /temp - LoadData.jar, Driver.jar, DatabaseState.jar, MultiProcessDriver.jar, SingleDriver.jar - these jars are already available in first 2 nodes in cluster
cp -rpf /home/stuproj/cs4224m/*.jar /temp

5. Copy project-files directory given into /temp folder - already done in node 1 and node 2
cp -rpf /home/stuproj/cs4224m/project-files /temp

6. Remove existing output folder if it exists
rm -rf /temp/output

### Exectution

1. Set environment variables
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.222.b10-1.el7_7.x86_64
export PATH=$JAVA_HOME/bin:$PATH
export CASSANDRA_HOME=/temp/Cassandra
export PATH=$CASSANDRA_HOME/bin:$PATH
export PATH=/home/stuproj/cs4224m/apache-maven-3.6.2/bin:$PATH

2. Run LoadData executable jar file - this loads all data into cassandra database
cd /temp
chmod 777 LoadData.jar
java -jar LoadData.jar

3. Run transactions 
If number of clients is 10 or 20, from node 1 run the following commands

cd /temp
chmod 777 Driver.jar
java -jar Driver.jar 10 "QUORUM" "QUORUM"
Note: You can change the value for number of clients, read consistency and write consistency in the above command.

If number of clients is 40, from node 1 and node 2 run the following commands concurrently
We have put the last 20 transaction files as first 20 files in node 1, so this should work similar to executing with 40 "QUORUM" "QUORUM"

cd /temp
chmod 777 Driver.jar
java -jar Driver.jar 20 "QUORUM" "QUORUM"
Note: You can change the value for number of clients, read consistency and write consistency in the above command.

After you execute the transactions, the output can be found in output folder for stdOut and stdErr of each process. 
Minimum, maximum transaction throughput is displayed in the stdOut 

4. Run database state
cd /temp
chmod 777 DatabaseState.jar
java -jar DatabaseState.jar

All the queries are dislplayed in stdOut
If neede in a file 
java -jar DatabaseState.jar > dbState


