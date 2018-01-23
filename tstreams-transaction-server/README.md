# tstreams-transaction-server
Implements Transaction storage server for T-Streams (hereinafter - TTS)

## Table of contents

- [Launching](#launching)
    - [Configuration](#configuration)
        - [General](#general)
        - [Multinode](#multinode)
        - [Tracing](#tracing)
    - [Preparation](#preparation)
    - [Java](#java)
    - [Docker](#docker)
        - [Environment variables](#environment-variables)
- [License](#license)

## Launching

There is two ways to launch TTS:
- via _java_ command
- via _docker_ image

You should pass a file with properties in both cases. The file should contain the following properties:

### Configuration

#### General

|NAME                                               |DESCRIPTION    |TYPE           |EXAMPLE        |VALID VALUES|
| --------------------------------------------------| ------------- | ------------- | ------------- | ------------- |
| server.type                                       | The type of server. |string|multinode|one of: singlenode, multinode, common, checkpoint-group|
| bootstrap.host                                    | ipv4 or ipv6 listen address. |string | 127.0.0.1| |
| bootstrap.port                                    | Port to a server binds.  |int    |8071| |
| bootstrap.open-ops-pool-size                      | Size of the ordered pool that contains single thread executors to work with some producer transaction operations. |int | Runtime.getRuntime.availableProcessors() |positive integer |
| subscribers.update-period-ms                      | Delay in milliseconds between updates of current subscribers online. | int | 1000| positive integer|
| subscribers.monitoring-zk-endpoints               | The ZooKeeper server(s) connect to |string| 127.0.0.1:2181,127.0.0.1:2182 |[ipv4 or ivp6]:[port],[ipv4 or ivp6]:[port]* |
| authentication.key                                | The key to authorize server's clients.  |string |key| |
| authentication.key-cache-size                     | The number of active tokens a server can handle over time.  |int    |10000| [1,...]|
| authentication.key-cache-expiration-time-sec      | The lifetime of token after last access before expiration.  |int    | 600| [1,...]|
| storage.file-prefix                               | The path where folders of Commit log and rocksdb databases would be placed. Should be absolute |string |/tmp| |
| storage.streams.zk-directory                      | The zooKeeper path for stream entities. | string | /tts/streams | all path starts with '/' and separated with the same character |
| storage.data.directory                            | The subfolder of 'storage.file-prefix' where rocksdb databases are placed which contain producer data.  |string |transaction_data| |
| storage.metadata.directory                        | The subfolder of 'storage.file-prefix' where rocksdb database is placed which contains producer and consumer transactions.  |string |transaction_metadata| |
| storage.commit-log.raw-directory                  | The subfolder of 'storage.file-prefix' where commit log files are placed.  |string |commmit_log| |
| storage.commit-log.rocks-directory                | The subfolder of 'storage.file-prefix' where rocksdb database is placed which contains commit log files. |string |commit_log_rocks| |
| storage.data.compaction-interval                  | Seconds between data compaction. The compaction process is required to delete expired entries from: 1) RocksDB (i.e. data and metadata of transactions); 2) Bookkeeper (entry logs); 3) Zookeeper (ledgers meta) |integer|3600|positive integer|
| rocksdb.write-thread-pool                         | The number of threads of pool are used to do write operations to Rocksdb databases.|int    | 4| [1,...]|
| rocksdb.read-thread-pool                          | The number of threads of pool are used to do read operations from Rocksdb databases.|int    | 2| [1,...]|
| rocksdb.transaction-ttl-append-sec                | The value that is added to stream TTL in order to determine data lifetime of producer transactions. This value is common for all streams. As the stream TTL value could be different for each stream, the data lifetime would be different for each stream too. |int    | 1| [1,...]|
| rocksdb.transaction-expunge-delay-sec             | The lifetime of a transaction metadata after persistence to database (6 months by default). If negative integer - transactions aren't deleted at all. You shouldn't set the stream TTL value more than this setting to avoid a case when transactions metadata will have been deleted before transactions data. |int    | 180 * 24 * 60 * 60 | integer |
| rocksdb.max-background-compactions                | The maximum number of concurrent background compactions. The default is 1, but to fully utilize your CPU and storage you might want to increase this to approximately number of cores in the system.  |int    | 1| [1,...]|
| rocksdb.compression-type                          | Compression takes one of values: [NO_COMPRESSION, SNAPPY_COMPRESSION, ZLIB_COMPRESSION, BZLIB2_COMPRESSION, LZ4_COMPRESSION, LZ4HC_COMPRESSION]. If it's unimportant use a *LZ4_COMPRESSION* as default value.  |string |LZ4_COMPRESSION| |
| rocksdb.is-fsync                                  | If true, then every store to stable storage will issue a fsync. If false, then every store to stable storage will issue a fdatasync. This parameter should be set to true while storing data to filesystem like ext3 that can lose files after a reboot.   |boolean| true| |
| zk.endpoints                                      | The socket address(es) of ZooKeeper servers.  |string |127.0.0.1:2181| |
| zk.common.prefix                                  | The coordination path to retrieve/persist socket address of t-streams transaction server.  |string |/tts/master | |
| zk.common.election-prefix | The coordination path is used for leader election among common servers. | string | /tts/election |  |
| zk.checkpointgroup.prefix | The coordination path is used for providing current master/leader checkpoint group server. | string | /tts/cg |  |
| zk.checkpointgroup.election-prefix | The coordination path is used for leader election among checkpoint group servers. | string | /tts/cgelection |  |
| zk.session-timeout-ms                             | The time to wait while trying to re-establish a connection to a ZooKeeper server(s).  |int    | 10000| [1,...]|
| zk.connection-retry-delay-ms                      | Delay between retry attempts to establish connection to ZooKeepers server on case of lost connection.  |int    | 500| [1,...]|
| zk.connection-timeout-ms                          | The time to wait while trying to establish a connection to a ZooKeeper server(s) on first connection.  |int    | 10000| [1,...]|
| network.max-metadata-package-size                 | The size of metadata package that client can transmit or request to/from server, i.e. calling 'scanTransactions' method. If client tries to transmit amount of data which is greater than maxMetadataPackageSize or maxDataPackageSize then it gets an exception. If server receives a client requests of size which is greater than maxMetadataPackageSize or maxDataPackageSize then it discards them and sends an exception to the client. If server during an operation undertands that it is near to overfill constraints it can stop the operation and return a partial dataset. |int    | 10000| [1,...]|
| network.max-data-package-size                     | The size of data package that client can transmit or request to/from server, i.e. calling 'getTransactionData' method. If client tries to transmit amount of data which is greater than maxMetadataPackageSize or maxDataPackageSize then it gets an exception. If server receives a client requests of size which is greater than maxMetadataPackageSize or maxDataPackageSize then it discards them and sends an exception to the client. If server during an operation undertands that it is near to overfill constraints it can stop the operation and return a partial dataset. |int    | 10000| [1,...]|
| commit-log.write-sync-policy                      | Policies to work with commit log. If 'every-n-seconds' mode is chosen then data is flushed into file when specified count of seconds from last flush operation passed. If 'every-new-file' mode is chosen then data is flushed into file when new file starts. If 'every-nth' mode is chosen then data is flushed into file when specified count of write operations passed.  |string    | every-nth| [every-n-seconds, every-nth, every-new-file]|
| commit-log.write-sync-value                       | Count of write operations or count of seconds between flush operations. It depends on the selected policy |int    | 10000| [1,...]|
| commit-log.incomplete-read-policy                 | Policies to read from commit log files. If 'resync-majority' mode is chosen then ???(not implemented yet). If 'skip-log' mode is chosen commit log files than haven't md5 file are not read. If 'try-read' mode is chosen commit log files than haven't md5 file are tried to be read. If 'error' mode is chosen commit log files than haven't md5 file throw throwable and stop server working. |string |error |[resync-majority (mandatory for replicated mode), skip-log, try-read, error] |
| commit-log.close-delay-ms                         | The time through which a commit log file is closed. |int  |200    |
| commit-log.rocksdb-expunge-delay-sec              | The lifetime of commit log files before they are deleted. | int | 86400 |
| commit-log.zk-file-id-gen-path                    | The coordination path for counter that is used to generate and retrieve commit log file id. | string | /server_counter/file_id_gen |

It isn't required to adhere the specified order of the properties, it's for example only. 
But all this properties should be defined with the exact names and appropriate types.

#### Multinode

This properties is required for multinode server.

| NAME | DESCRIPTION | TYPE | EXAMPLE |
| --- | --- | --- | --- |
| ha.common.zk.path | The coordination path to TreeList for common group. | string | /tts/commonmaster_tree |
| ha.common.last-closed-ledger | The coordination path for last closed ledger for common group. | string | /tts/commonlast_closed_ledger |
| ha.common.close-delay-ms | The delay between creating new ledgers. | int | 200 |
| ha.cg.zk.path | The coordination path to TreeList for checkpoint group. | string | /tts/cgmaster_tree |
| ha.cg.last-closed-ledger | The coordination path for last closed ledger for checkpoint group. | string | /tts/cglast_closed_ledger |
| ha.cg.close-delay-ms | The delay between creating new ledgers. | int | 200 |
| ha.ensemble-number | The number of bookies the data in the ledger will be stored on. | int | 3 |
| ha.write-quorum-number | The number of bookies each entry is written to. | int | 3 |
| ha.ack-quorum-number | The number of bookies we must get a response from before we acknowledge the write to the client. | int | 2 |
| ha.password | The password to access constructed ledgers and create new ledgers. | string | bkpassword |
| ha.expunge-delay-sec | The lifetime of a ledger in seconds (6 months by default). If negative integer - ledgers aren't deleted at all. | int | 86400 |

#### Tracing

To enable tracing to [OpenZipkin](http://zipkin.io) server set the
following properties:

| NAME | DESCRIPTION | TYPE | EXAMPLE |
| --- | --- | --- | --- |
| tracing.enabled | If true, tracing is enabled (false by default). | string | true |
| tracing.endpoint | OpenZipkin server address. | string | localhost:9411 |


### Preparation

Run that command from a project root directory to build a server:

```bash
sbt tstreams-transaction-server/assembly
```

or if you want to skip tests:

```bash
sbt 'set (test in assembly) in tStreamsTransactionServer := {}' tstreams-transaction-server/assembly
```


### Java

In addition to the properties file you should provide dependency through
adding jar of 'slf4j-log4j12-1.7.25' to a classpath, to launch TTS.
That is run the following command:

```bash
java -Dconfig=<path_to_config>/config.properties -cp <path_to_TTS_jar>/tstreams-transaction-server-<version>.jar:<path_to_slf4j_impl_jar>/slf4j-log4j12-1.7.25.jar com.bwsw.tstreamstransactionserver.ServerLauncher
```

To use your own logging configuration write configuration file
[log4j.properties](src/main/resources/log4j.properties) and add a java
option `-Dlog4j.configurationFile=<path-to-log4j-properties>`.

### Docker

To build docker image:

```bash
docker build --build-args version=<version> --tag bwsw/tstreams-transaction-server .
```

To download image use:
```bash
docker pull bwsw/tstreams-transaction-server
```

To run docker image you should provide a path to config directory where
a file named `config.properties` is, `bootstrap.port` in `config.properties`
file should be `8071`:

```bash
docker run -v <path_to_config_properties>:/etc/conf/config.properties [-v <path_to_storage_dir>:<storage.file-prefix>] [-p <external_port>:8071] bwsw/tstreams-transaction-server
```

- `<path_to_config_properties>` &mdash; absolute path to `config.properties` file (e.g. `${PWD}/config.properties`);
- `<path_to_storage_dir>` &mdash; path to storage directory;
- `<storage.file-prefix>` &mdash; value of `storage.file-prefix` configuration.

To use your own logging configuration add volume with logging configuration
file (e.g. `-v ${PWD}/log4j.properties:/etc/conf/log4j.properties`) and
put container path to configuration file to environment variable
LOG4J_PROPERTIES_FILE (e.g. `-e LOG4J_PROPERTIES_FILE=/etc/conf/log4j.properties`)


#### Environment variables:

| Name | Description | Default |
| --- | --- | --- |
| CONFIG_FILE | Path to configuration file | /etc/conf/config.properties |
| LOG4J_PROPERTIES_FILE | Path to custom logging configuration file ([log4j.properties](src/main/resources/log4j.properties)). |  |

## License

Released under Apache 2.0 License
