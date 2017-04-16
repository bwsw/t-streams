# T-streams

T-streams library implements transactional persistent queues for exactly-once, batch message exchange in PUB-SUB mode.

## Introduction

T-streams is an embeddable library for JVM environment designed for atomic messaging between network agents via transactions in PUB-SUB mode. The library is purposed for reliable message delivery with queue persistence and replay support. It has unique features as compared to the Kafka broker and may be used to replace it in message processing pipelines where exactly-once processing is required. 

T-streams is the transport core of another product of ours, SJ-Platform.

For messaging we (at Bitworks Software) often use such messaging systems as Kafka, 0MQ, RabbitMQ and others, depending on the objectives to be solved. When it comes to processing data streams for BigData, Kafka is the default industry standard supported by many processing systems, such as Apache Spark, Flink, Samza. Kafka is an excellent product that we widely use where it provides sufficient functionality. However, various problems exist that Kafka (unfortunately) cannot tackle. The main one (in our opinion) is the inability to implement exactly-once processing with Kafka, if events are fed from multiple Kafka topics and then transformed and relayed to other Kafka topics.

The inability to do this transaction-wise has led us to the idea and motivated to create a solution that would provide for the solution to the problem. This is how the T-streams library appeared. T-streams is implemented on Scala v2.12 and has only one external dependency Apache Zookeeper which is used to maintain the discovery. It is available under Apache License v2. You can learn more about T-streams on the product web site.

T-streams implements messaging for Producers and Consumers which is critical for CEP (complex event processing) systems. When creating T-streams, we were inspired by Apache Kafka and shares similar concepts.  T-streams library is suited to store millions and billions of queues. It allows developers to do exactly-once and at-least-once processing when T-streams is both the queue's source and its destination. The library fits great for batch processing where each batch includes multiple items (messages, events, etc.). 

Web-site: http://t-streams.com/

## T-streams library fits good if…

T-streams library is designed to solve the problems, mentioned before and provides following features:

 * exactly-once and at-least-once processing models when source and destination queues are T-streams;
 * gives developers the way to implement exactly-once and at-least-once processing models when source is T-streams and destination is any key-value store (or full-featured DBMS, e.g. Oracle, MySQL, Postgres, Elasticsearch);
 * is designed for batch processing where batch includes some items (events, messages, etc.);
 * scales perfectly horizontally;
 * scales perfectly in direction of amount of queues (supports millions of queues without problems);
 * allows consumers to read from most recent offset, arbitrary date-time offset, most ancient offset.
 * provides Kafka-like features:
    * Reading data by multiple consumers from one queue;
    * Writing data by multiple producers t one queue;
    * Use of the streams with one partition or many partitions.
    * Stores data until expiration date-time set by stream configuration.

## License

T-streams library is licensed under [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).

## Author

T-streams library is created by [Bitworks Software, Ltd.](http://bw-sw.com)

mailto: bitworks (at) bw-sw.com
