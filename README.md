# T-streams

T-streams library implements transactional persistent queues for exactly-once, batch message exchange in PUB-SUB mode.

## Introduction

T-streams is an embeddable library for JVM designed for messaging between distributed agents via transactions in PUB-SUB mode. T-streams is designed for reliable message delivery with queue persistence and replay support. T-streams has unique features as compared to the Kafka broker and may be used to replace it in message sequences where exactly-once processing is required. T-streams is the transport kernel of another product of ours, Juggler.

For implementing messaging within data streams we often use such messaging systems as Kafka, 0MQ, RabbitMQ and others, depending on the objectives to be solved. When it comes to processing data streams for BigData, Kafka is the default industry standard supporting many processing systems, such as Apache Spark, Flink, Samza. Kafka is an excellent product that we admire and happily use where it provides sufficient functionality. However, various problems exist that Kafka (unfortunately) cannot tackle. The main one (in our opinion) is the inability to implement exactly-once processing with Kafka, if events are fed from multiple Kafka topics and then transformed and relayed to other Kafka topics.

The inability to do this transaction-wise has led us to the idea and urge to create a solution that would dispense with the above flaw and provide for the solution to the problem. This is how the T-streams library came about. T-streams is implemented on Scala v2.11 and operates based on Apache Cassandra, Zookeeper and Aerospike.

The T-streams library is available under Apache License v2. You can learn more about T-streams on the product web site.

T-streams implements messaging for Producers and Consumers which is critical for CEP (complex event processing) systems. When creating T-streams, we were inspired by Apache Kafka. We took the drawbacks of Kafka and fixed them in T-streams. T-streams libraries are suited to store millions and billions of queues. T-streams allows developers to do exactly-once and at-least-once processing when T-streams is both the queue's source and its destination. Our libraries are also good for batch processing where each batch includes multiple items (messages, events, etc.). The system is scalable both horizontally (Aerospike clustering, Cassandra clustering) and in relation to the number of queues. In addition to this, the T-streams library allows the Consumer to read starting from the most recent change /from any change /from the very first change. Apart from that, the system has Kafka-like functionality. For example, it allows data to be read from multiple Producers and Consumers on the same queue. It also allows for using streams from one or multiple partitions.

Web-site: http://t-streams.com/

## T-streams library fits good if…

T-streams library is designed to solve the problems, mentioned before and provides following features:

 * exactly-once and at-least-once processing models when source and destination queues are T-streams;
 * gives developers the way to implement exactly-once and at-least-once processing models when source is T-streams and destination is any key-value store (or full-featured DBMS, e.g. Oracle, MySQL, Postgres, Elasticsearch);
 * is designed for batch processing where batch includes some items (events, messages, etc.);
 * scales perfectly horizontally (Aerospike clustering, Cassandra clustering);
 * scales perfectly in direction of amount of queues (supports millions of queues without problems);
 * relies on Cassandra replication and Aerospike replication for replication and consistency;
 * allows consumers read from most recent offset, arbitrary date-time offset, most ancient offset.
 * provides Kafka-like features:
    * It allows to read data by multiple consumers from one queue;
    * It allows to write data by multiple producers t one queue;
    * It allows to use streams with one partition or many partitions.
    * It stores data until expiration date-time set by stream configuration.

## License

T-streams library is licensed under [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).

## Author

T-streams library is created by [Bitworks Software, Ltd.](http://bw-sw.com)

mailto: bitworks (at) bw-sw.com
