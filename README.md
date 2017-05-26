# T-streams

T-streams library implements transactional persistent queues for exactly-once, batch message exchange in PUB-SUB mode.

## Introduction

T-streams (transactional streams) is a Scala library and infrastructure components which implement transactional messaging, important for many practical applications including CEP (complex event processing), financial and business-critical systems that must handle an event exactly-once and sometimes at-least-once or at-most-once.

Basically, T-streams is designed for exacly-once processing (so it includes idempotent producer, consumer and subscriber). T-streams conceptual design is inspired by Apache Kafka.

T-streams library uses robust data management systems for operation:

* Apache Zookeeper – distributed coordination
* Apache BookKeeper – eventually consisted replicated commit log system
* RocksDB – operational storage

It’s implemented with Scala 2.12, Apache Curator, Google Netty, Google Guava and other standard sound components purposed for the development of reliable distributed applications.

Web-site: http://t-streams.com/

## Short About

In a nutshel T-streams infrastructure functions as and can be described as:

1. a distributed, replicated message broker and clients library for JRE (like a Kafka, RabbitMQ, etc.)
2. is designed for reliable intranet (not public Internet), so network failures can lead to overall performance (but not reliability) degradation because of UDP usage for PUB-SUB and other operations
3. implements basic token based authentication
4. events are handled as atomic transactions
5. transactions are
    1. long-living or short living
    2. stored in streams and their partitions
    3. are strictly ordered inside a partition according to the transaction open time (who opens first will be processed first)
    4. persistent, stored in the replicated datastore and evicted after specified period of time, e.g. week or month.
    5. persisted with checkpoint operation
    6. cancelled with cancel operation
    7. designed to build idempotent producers
    8. processed by 
        1. idempotent consumers which implement polling processing
        2. subscribers which implement pub-sub processing
6. consumers and subscribers
    1. poll or pub-sub transactions from
        1. most recent offset
        2. arbitrary date-time offset
        3. most ancient offset
        4. last fixed offset
    2. can read transaction by certain transaction ID
    3. can read transactions in the range
    4. fix their current state with checkpoint operation
7. Checkpoint operation is atomic for
    1. a single transaction when is called for a transaction object
    2. for all opened transaction of a producer when is called for a producer object
    3. for all opened transaction for certain partition of a producer when is called for a producer object
    4. for all producers, consumers and subscribers which are participants of a CheckpointGroup when is called for CheckpointGroup object

## License

T-streams library is licensed under [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).

## Author

T-streams library is created by [Bitworks Software, Ltd.](http://bw-sw.com)

mailto: bitworks (at) bw-sw.com
