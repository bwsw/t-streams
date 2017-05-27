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

## License

T-streams library is licensed under [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).

## Author

T-streams library is created by [Bitworks Software, Ltd.](http://bw-sw.com)

mailto: bitworks (at) bw-sw.com
