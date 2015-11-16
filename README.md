# kafka-avro-consumer

Demonstrates how to consume avro messages on a *NIX/MacOS platform.

## Pre-requisites

* Java, maven, git client, etc. have already been installed

## Kafka Installation

* Download kafka (for this project I am using kafka_2.10-0.8.2.2)
* Untar the downloaded file: this projects assumes `/opt` directory: `cd /opt; tar xzvf ~/Downloads/kafka_2.10-0.8.2.2.tgz`
* Create a symbolic link in opt directory: `ln -sf kafka_2.10-0.8.2.2 kafka`
* Create appropriate aliases for starting kafka components. Here are some of my aliases (default from kafka project wiki):

````
% alias start_zookeeper='/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties'
% alias start_broker='/opt/kafka/bin/kafka-server-start.sh  /opt/kafka/config/server.properties'
% alias create_topic='/opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic kafkatopic'
% alias list_topic='/opt/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181 kafkatopic'
% alias start_producer='/opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic kafkatopic'
% alias start_consumer='/opt/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic kafkatopic --from-beginning'
````    

## Usage 

* Download this project to a directory as appropriate:

````
% cd workspace
% git clone https://github.com/ypant/kafka-avro-consumer.git
````    

* Maven project file structure: 

````
./LICENSE
./pom.xml
./README.md
./src
./src/main
./src/main/java
./src/main/java/com
./src/main/java/com/example
./src/main/java/com/example/AvroConsumer.java
./src/main/resources
./src/main/resources/log4j.xml
./src/main/resources/schema
./src/main/resources/schema/book.avsc
````

* Start Kafka components in separate terminals:

````
% start_zookeeper
% start_broker
% create_topic and list_topic - to verify that "kafkatopic" has been created
% start_consumer
% start_producer: Make sure that the messages sent from the producer window appears in the consumer window
````

* Compile and execute the program using maven (from project root directory):
````
% mvn clean compile
% mvn exec:java -Dexec.mainClass="com.example.AvroConsumer" -Dexec.args="localhost:2181 test-consumer-group kafkatopic"
````
And generate events using AvroProducer. Project available at: https://github.com/ypant/kafka-avro-producer.git

## History

* 15 Nov 2015 - Initial Revision


## License
[MIT License](LICENSE)



