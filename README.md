# T-streams

T-streams is scalable, transactional persistent queues for exactly-once, batch messaging.

# Introduction

T-streams (transactional streams) is a Scala library which implements transactional messaging for Producers and Consumers, which is important for CEP (complex event processing) systems that must handle an event exactly-once, at-least-once or at-most-once. T-streams implementation is inspired by Apache Kafka.

T-streams library uses next data management systems for operation:

 * Apache Cassandra – metadata backend, data backend;
 * Aerospike – data backend (preferred);
 * Redis/Apache Zookeeper – distributed lock management and pub-sub features. Redis is preferred.

Data backends are pluggable and can be developed for other key-value storages, e.g. Redis or even Ceph, AWS S3, etc.

# T-streams features

Usually developers choose Apache Kafka for storing data in data processing systems – it is robust, it scales well and fits in many cases.

We started T-streams as a replacement for Apache Kafka, which is perfect but has lack of  features which exist in T-streams. Kafka is widely used but doesn’t fit well for CEP systems where processing chain can be complex and involve kafka-to-kafka processing.

Kafka allows to place somewhere (usually Zookeeper) an offset from which consumer read data, but it doesn’t provide any mechanism for producers, as a result exactly-once processing is impossible with Kafka when it’s used in kafka-to-kafka processing.

Also, with Kafka it is impossible to start consuming from point of time natively because Kafka’s offset doesn’t match to time.

Another problem which exists in Kafka begins from the way it stores queues, it doesn’t fit well for storing millions or even billions of queues because queues are matched to separate files.

# T-streams library fits good if…

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
