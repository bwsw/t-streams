TStreamsFactory
================

.. Contents::

TStreams Factory class is a general purpose factory which should be used to create top-level T-streams objects.

To start using the class it must be imported with next line *(need updating)*::

 import com.bwsw.tstreams.env.{TStreamsFactory, TSF_Dictionary}

As shown in the example above there are two classes – the object class ``TSF_Dictionary`` and the regular class ``TStreamsFactory`` that are explained below.

TStreamsFactory Class
------------------------

A top level “god-like” class that manages the creation of producers, subscribers, consumers, and streams.

Methods
~~~~~~~~~~~~~~~~~

Constructor
""""""""""""""""""

::
	
 val factory = new TStreamsFactory()

The constructor allows creating the object for further usage.

setProperty method
"""""""""""""""""""""
The method allows setting factory properties which are used when the factory generates an object. It has next signature::
 
 def setProperty(key: String, value: Any):TStreamsFactory

Please, note that the method returns the same factory object which gives a developer a way to chain several **setProperty** methods together as shown in the next snippet *(need updating)*::

 factory.setProperty(TSF_Dictionary.Metadata.Cluster.NAMESPACE, Setup.KS).                 
     setProperty(TSF_Dictionary.Data.Cluster.NAMESPACE, "test").                    
     setProperty(TSF_Dictionary.Producer.BIND_PORT, 18001).                          
     setProperty(TSF_Dictionary.Consumer.Subscriber.BIND_PORT, 18002).               
     setProperty(TSF_Dictionary.Consumer.Subscriber.PERSISTENT_QUEUE_PATH, null).  
     setProperty(TSF_Dictionary.Stream.NAME, "test-stream").                          
     setProperty(TSF_Dictionary.Consumer.Subscriber.POLLING_FREQUENCY_DELAY, 1000)

.. important:: If the factory instance is “locked” (see next) **setProperty** will raise **IllegalStateException**.

.. important:: If an unknown key is used with **setProperty** method, it will raise **IllegalArgumentException**. For all possible keys see TSF-Dictionary_ object next.

Keep in mind that almost all of the default `TStreamsFactory` settings are good and you do not need to change them. Below (in the chapter for the TSF-Dictionary_ object), all the options are described and those that should be changed are outlined with **bold**.

getProperty method
""""""""""""""""""""""

The method is not widely used. It has the next signature::

 def getProperty(key: String): Any

In general, a developer should use it only to get a default value which he (or she) does not set but which is set for the system by default.

lock method
"""""""""""""

The lock method should be used to freeze factory options and prevent them from further alterations. It can be used when a developer configures a basic "golden image" factory object. The method has the next signature::

 def lock():Unit

Important notice: if the factory instance is “locked” (see later) **setProperty** will raise **IllegalStateException**.

Usage example:: 

 factory.lock()

copy method
"""""""""""""""
The copy method is useful when a developer wishes to create a copy of a factory object from an existing factory object (actually the idea is to create an alterable factory object from a locked "golden image" factory object). The method has the next signature::

 def copy(): TStreamsFactory

Usage example::

 val newfactory = factory.copy()

getDataStorage method
""""""""""""""""""""""""

*(not used now?)*

The method returns IDataStorage object. Normally a developer does not use the method because IDataStorage object is used by producers, subscribers, consumers internally. The method has the next signature::

 def getDataStorage(): IStorage[Array[Byte]]

getDataStorage method
"""""""""""""""""""""""""""""

*(not used now?)*

The method returns IDataStorage object. Normally a developer does not use the method because IDataStorage object is used by producers, subscribers, consumers internally. The method has the next signature::

 def getDataStorage(): IStorage[Array[Byte]]

getMetadataStorage method
""""""""""""""""""""""""""""

*(not used now?)*

The method returns MetadataStorage object. Normally a developer does not use the method because MetadataStorage object is usedВ by producers, subscribers, consumers internally. The method has the next signature::

 def getMetadataStorage(): MetadataStorage

getStream method
""""""""""""""""""""""""

*(not used now?)*

The method returns TStream object. Normally a developer does not use the method because MetadataStorage object is usedВ by producers, subscribers, consumers internally. The method has the next signature::
	
 def getStream(): TStream[Array[Byte]]

getProducer method
""""""""""""""""""""""

*(updated)*

The method returns a producer object instance. It should be used to create new producers. The method has the next signature::

 def getProducer(name: String, partitions: Set[Int]):Producer

where

.. csv-table:: 
 :header: "Parameter", "Meaning", "Domain", "Example"
 :widths: 10, 15, 25, 30

 "name", "AВ name of the producer object.", "short string", ""
 "partitions", "В A set of partitions which the producer will use in operation", "Set[Int]", "Set(0)"
 
An example of the usage is demonstrated below *(need updating)*::

 import com.bwsw.tstreams.generator.LocalTransactionGenerator
 import com.bwsw.tstreams.converter.StringToArrayByteConverter

 val producer = factory.getProducer[String](
                 name          = "producer-1",                     
                 transactionGenerator  = new LocalTransactionGenerator,  
                 converter     = new StringToArrayByteConverter, 
                 partitions    = Set(0),                       
                 isLowPriority = false)

All the parameters like stream name, contact points for data and metadata stores are received from the factory. The method only specifies current producer-related parameters. So, for example, a developer also could request another producer for partition 1, etc.

getConsumer method
"""""""""""""""""""""""
*(updated)*

The method returns a consumer object instance (it is for a **polling** interface, for a **pub-sub** interface look at the getSubscriber method below). It should be used to create new consumers. The method has the next signature::

 def getConsumer(name: String, partitions: Set[Int],offset: IOffset, useLastOffset: Boolean = true,checkpointAtStart: Boolean = false): Consumer

where

.. csv-table:: 
 :header: "Parameter", "Meaning", "Domain", "Example"
 :widths: 10, 15, 45, 30

 "name", "a name of the consumer object", "short string", ""
 "partitions", "A set of partitions from which the consumer will read transactions", "Set[Int]", "Set(0)"
 "offset", "From what historical position the consumer will read transactions", "IOffset", "Oldest"
 "useLastOffset", "Is a point the consumer should start read the data from, the point where it was stopped crashed last time (where the last checkpoint had occurred)", "Boolean", "true"
 "checkpointAtStart", "", "Boolean", "false"

An example of the usage is demonstrated next *(need updating)*::

 import com.bwsw.tstreams.generator.LocalTransactionGenerator
 import com.bwsw.tstreams.converter.ArrayByteToStringConverter
 import com.bwsw.tstreams.agents.consumer.Offset.Newest
 
 
 val consumer = factory.getConsumer[String](
       name            = "consumer-1",              
       transactionGenerator    = new LocalTransactionGenerator,
       converter       = new ArrayByteToStringConverter,
       partitions      = Set(0),
       offset          = Newest,
       isUseLastOffset = false)

All the information like stream name, contact points for data and metadata stores are received from the factory. The method only specifies current consumer-related parameters. So, for example, a developer also could request another one consumer for partition 1, etc.

getSubscriber method
""""""""""""""""""""""""

*(upsdated)*

The method returns subscriber object instance (it is for **pub-sub** interface, for **polling** interface look at the getConsumer method before). It should be used to create new subscribers. The method has the next signature::

 def getSubscriber(name: String, partitions: Set[Int],callback: Callback, offset: IOffset, useLastOffset:Boolean = true, checkpointAtStart: Boolean =false): Subscriber
 
where

.. csv-table:: 
 :header: "Parameter", "Meaning", "Domain", "Example"
 :widths: 10, 15, 45, 30

 "name", "a name of the consumer object", "short string", ""
  "partitions", "A set of partitions from which consumer will read transactions", "Set[Int]", "Set(0)"
 "callback", "A callback that will be called when aВ new transaction will be ready for processing", "Callback[T]", "
 
 ::

 new Callback[String] {
        override def onTransaction(
              op:                 TransactionOperator[String], 
              transaction:     Transaction[String]): Unit = {
              // some stuff
        }
 }"
 "offset", "From what historical position consumer will read transactions", "IOffset", "Oldest"
 "useLastOffset", "Is consumer should start read the data from the point where it was stopped, crashed last time (where the last checkpoint had occurred)", "Boolean"
 "checkpointAtStart", "", "Boolean", "false"
 
.. note:: TransactionOperator is the interface for Consumer and has the same methods. Actually, a developer can cast it to the Consumer class instance.	

close method
"""""""""""""""
The close method is used to end the factory operation, all further calls to methods will raise **IllegalStateException**::

 def close(): Unit

Usage example::

 factory.close()

Methods To Add
""""""""""""""""""""

def getCheckpointGroup(executors: Int = 1):CheckpointGroup

def getStorageClient(): StorageClient

val isClosed: AtomicBoolean

val isLocked: AtomicBoolean

val co: ConfigurationOptions.type

TSF_Dictionary Object
----------------------------

*(now corresponds to TStreamsFactoryDefaults?)*

The object contains all valid options that can be used with setProperty_method_.

TSF_Dictionary.Metadata.Cluster keyset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The keyset includes parameters for the metadata storage. Parameters which are marked with bold should be set-up properly, other ones are for thin tuning.

.. csv-table:: 
 :header: "Key", "Textual constant", "Purpose", "Domain", "Example", "Default"
 :widths: 10, 15, 35, 20, 20, 10

 "**ENDPOINTS**", "metadata.cluster.endpoints", "C* ring where the metadata is stored", "Comma-separated string host:port,host:port,host:port", "1.1.1.1:9042,1.1.1.2:9042", "localhost:9042"
 "**NAMESPACE**", "metadata.cluster.namespace", "C* keyspace where the metadata is stored", "Valid keyspace string", "t_streams", "test"
 "**LOGIN**", "metadata.cluster.login", "C* login if required", "Valid login string", "cassandra", "null"
 "**PASSWORD**", "metadata.cluster.password", "C* password if required", "Valid password string", "secret", "null"
 "**LOCAL_DC**", "metadata.cluster.local-dc", "Local DC for DC-aware C* policy", "Valid DC name string", "dc1", "null"
 "KEEP_ALIVE_MS", "metadata.cluster.keep-alive-ms", "C* setting for keep-alive", "Number between 1000 and 10000", "2000", "5000"
 "MIN_RECONNECTION_DELAY_MS", "metadata.cluster.min-reconnection-delay-ms", "C* reconnection delay min", "Number between 500 and 5000", "1000", "1000"
 "MAX_RECONNECTION_DELAY_MS", "metadata.cluster.max-reconnection-delay-ms", "C* reconnection delay max", "Number between 5000 and 60000", "60000", "60000"
 "QUERY_RETRY_COUNT", "metadata.cluster.query-retry-count", "C* amount of retris for failed query", "Number between 1 and 20", "15", "10"
 "CONNECTION_TIMEOUT_MS", "metadata.cluster.connection-timeout-ms", "C* connection timeout", "Number between 1000 and 60000", "6000", "5000"
 "READ_TIMEOUT_MS", "metadata.cluster.read-timeout-ms", "C* read timeout", "Number between 10000 and 600000", "120000", "120000"
 "LOCAL_CONNECTIONS_PER_HOST", "metadata.cluster.local-connections-per-host", "C* connections amount to LOCAL hosts", "A pair (core, max)", "(8, 64)", "(4, 32)" 	
 "REMOTE_CONNECTIONS_PER_HOST", "metadata.cluster.remote-connections-per-host", "C* connections amount to REMOTE hosts", "A pair (core, max)", "(4, 32)", "(2, 16)" 	
 "LOCAL_REQUESTS_PER_CONNECTION", "metadata.cluster.local-requests-per-connection", "C* amount of simultaneous requests per one connection to LOCAL hosts", "Integer", "16000", "32576" 	
 "REMOTE_REQUESTS_PER_CONNECTION", "metadata.cluster.remote-requests-per-connection", "C* amount of simultaneous requests per one connection to REMOTE hosts", "Integer", "4000", "8192" 	
 "HEARTBEAT_INTERVAL_SECONDS", "metadata.cluster.heartbeat-interval-seconds", "C* driver idle heartbeat", "Integer", "5", "10" 	
 "CONSISTENCY_LEVEL", "metadata.cluster.consistency-level", "C* cluster required consistency level", "Enum, see C* documentation", "LOCAL_QUORUM", "ONE"

Textual constants can be used when the configuration is read from external files but for in-code style it is better to use predefined constants.

TSF_Dictionary.Data.Cluster keyset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The keyset includes parameters for Metadata storage. Parameters which are marked with bold should be set-up properly, other ones are for thin tuning.

.. csv-table:: 
 :header: "Key", "Textual constant", "Purpose", "Domain", "Example", "Default", "Comments"
 :widths: 10, 15, 35, 20, 20, 10, 20

 "Consts.DATA_DRIVER_AEROSPIKE", "aerospike", "Aerospike engine is used to store data", "aerospike", "aerospike", "aerospike", "" 	
 "Consts.DATA_DRIVER_CASSANDRA", "cassandra", "Cassandra engine is used to store data", "cassandra", "cassandra", "cassandra", "" 	
 "Consts.DATA_DRIVER_HAZELCAST", "hazelcast", "Hazelcast engine is used to store data", "hazelcast", "hazelcast", "hazelcast", "" 	
 "**DRIVER**", "data.cluster.driver", "Specify a data driver that is used to store data", "TSF_Dictionary.Data.Cluster.Consts.DATA_DRIVER _{AEROSPIKE, CASSANDRA, HAZELCAST}", "aerospike", "aerospike", "Hazelcast is trivial driver used for integration tests, Cassandra is for only-cassandra usage."
 "**ENDPOINTS**", "data.cluster.endpoints", "A cluster where data is stored", "Comma-separated string host:port,host:port,host:port", "1.1.1.1:9042,1.1.1.2:9042", "localhost:9042", ""  	
 "**NAMESPACE**", "data.cluster.namespace", "A keyspace", "Valid keyspace string for specified type of datastore", "t_streams", "test", ""  	
 "**LOGIN**", "data.cluster.login", "Login if required", "Valid login string", "cassandra", "null", "" 	
 "**PASSWORD**", "data.cluster.password", "Password if required", "Valid password string", "secret", "null", "" 	
 "**Cassandra.LOCAL_DC**", "data.cluster.cassandra.local-dc", "Local DC for DC-aware C* policy", "Valid DC name string", "dc1", "null", "" 	
 "Cassandra.KEEP_ALIVE_MS", "data.cluster.cassandra.keep-alive-ms", "C* setting for keep-alive", "Number between 1000 and 10000", "2000", "5000", "" 	
 "Cassandra.MIN_RECONNECTION_DELAY_MS", "data.cluster.cassandra.min-reconnection-delay-ms", "C* reconnection delay min", "Number between 500 and 5000", "1000", "1000", "" 	
 "Cassandra.MAX_RECONNECTION_DELAY_MS", "data.cluster.cassandra.max-reconnection-delay-ms", "C* reconnection delay max", "Number between 5000 and 60000", "60000", "60000", ""	
 "Cassandra.QUERY_RETRY_COUNT", "data.cluster.cassandra.query-retry-count", "C* amount of retris for failed query", "Number between 1 and 20", "15", "10", "" 	
 "Cassandra.CONNECTION_TIMEOUT_MS", "data.cluster.cassandra.connection-timeout-ms", "C* connection timeout", "Number between 1000 and 60000", "6000", "5000", "" 	
 "Cassandra.READ_TIMEOUT_MS", "data.cluster.cassandra.read-timeout-ms", "C* read timeout", "Number between 10000 and 600000", "120000", "120000", "" 	
 "Cassandra.LOCAL_CONNECTIONS_PER_HOST", "data.cluster.cassandra.local-connections-per-host", "C* connections amount to LOCAL hosts", "A pair (core, max)", "(8, 64)", "(4, 32)", "" 	
 "Cassandra.REMOTE_CONNECTIONS_PER_HOST", "data.cluster.cassandra.remote-connections-per-host", "C* connections amount to REMOTE hosts", "A pair (core, max)", "(4, 32)", "(2, 16)", "" 	
 "Cassandra.LOCAL_REQUESTS_PER_CONNECTION", "data.cluster.cassandra.local-requests-per-connection", "C* amount of simultaneous requests per one connection to LOCAL hosts", "Integer", "16000", "32576", "" 	
 "Cassandra.REMOTE_REQUESTS_PER_CONNECTION", "data.cluster.cassandra.remote-requests-per-connection", "C* amount of simultaneous requests per one connection to REMOTE hosts", "Integer", "4000", "8192", "" 	
 "Cassandra.HEARTBEAT_INTERVAL_SECONDS", "data.cluster.cassandra.heartbeat-interval-seconds", "C* driver idle heartbeat", "Integer", "5", "10", ""
 "Cassandra.CONSISTENCY_LEVEL", "data.cluster.cassandra.consistency-level", "C* cluster required consistency level", "Enum, see C* documentation", "LOCAL_QUORUM", "ONE", "" 	
 "Aerospike.WRITE_POLICY", "data.cluster.aerospike.write-policy", "Defines how aerospike will write data", "WritePolicy", "new WritePolicy(…)", "null", ""  	
 "Aerospike.READ_POLICY", "data.cluster.aerospike.read-policy", "Defines how aerospike will read data", "ReadPolicy", "new ReadPolicy(…)", "null", "" 	
 "Aerospike.CLIENT_POLICY", "data.cluster.aerospike.client-policy", "Defines how to connect", "ClientPolicy", "new ClientPolicy(…)", "null", "" 

Textual constants can be used when the configuration is read from external files but for in-code style it is better to use predefined constants.

TSF_Dictionary.Coordination keyset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The keyset defines where Zookeeper cluster is located and what prefix to use with Zookeeper.

.. csv-table:: 
 :header: "Key", "Textual constant", "Purpose", "Domain", "Example", "Default"
 :widths: 10, 15, 35, 20, 20, 10

 "**ENDPOINTS**", "coordination.endpoints", "Zookeeper cluster hosts", "string host1:port1,host2:port2,host3:port3,…", "localhost:2181,1.1.1.1:2181", "localhost:2181"
 "**ROOT**", "coordination.root", "Zookeeper prefix", "valid zookeper path string like /a/b/c", "/t-streams", "/t-streams"
 "TTL", "coordination.ttl", "Zookeeper session timeout", "Number between 1 and 10 seconds", "10", "5"
 "CONNECTION_TIMEOUT", "coordination.connection-timeout", "Zookeeper connection timeout", "Number between 1 and 10 seconds", "10", "5"
 "PARTITION_REDISTRIBUTION_DELAY", "coordination.partition-redistribution-delay", "Interval between every two updates on partition redistribution", "Number between 1 and 100 seconds", "10", "2"

TSF_Dictionary.Stream keyset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This scope is about a stream where producers, subscribers and consumers will operate. If several different streams are used then it is convinient to lock and copy the factory instance before every stream configuration.

.. csv-table:: 
 :header: "Key", "Textual constant", "Purpose", "Domain", "Example", "Default"
 :widths: 10, 15, 35, 20, 20, 10

 "**NAME**", "stream.name", "The stream name that will be used by producers, subscribers and consumers", "String", "mystream", "test"
 "**PARTITIONS**", "stream.partitions", "Total amount of partitions that stream can have", "Number > 0", "10", "1"
 "**TTL**", "stream.ttl", "How long transactions and their data will be available (before purge)", "Number > 60 seconds", "3600", "86400"
 "DESCRIPTION", "stream.description", "Custom description", "String", "My stream", "Test stream"

TSF_Dictionary.Producer keyset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The keyset is used to setup producer objects.

.. csv-table:: 
 :header: "Key", "Textual constant", "Purpose", "Domain", "Example", "Default"
 :widths: 10, 15, 35, 20, 20, 10

 "**THREAD_POOL**", "producer.thread-pool", "When producer acts like partition master thread pool defines amount of workers which handle workload", "Number between 1 and 64", "1", "4"
 "**THREAD_POOL_PUBLISHER_TREADS_AMOUNT**", "producer.thread-pool.publisher-threads-amount", "When producer acts like partition master amount of publisher threads in the thread pool definesВ how fast events will be delivered to subscribers. The parameter is important if there are a lot of subscribers for one producer", "Number between 1 and 32", "2", "1"
 "**BIND_HOST**", "producer.bind-host", "Host to bind to", "String, hostname or ip", "localhost", "localhost"
 "**BIND_PORT**", "producer.bind-port", "Port to bind to", "Number or (from, to)", "18000", "(40000,50000)"
 "TRANSPORT_TIMEOUT", "producer.transport-timeout", "Timeout defines response timeout after which master is considered to be unresponding", "Number, seconds between 1 and 10", "5", "5"
 "TRANSPORT_RETRY_COUNT", "producer.transport-retry-count", "Amount of retries if TRANSPORT_RETRY_COUNT achieved", "Number > 0", "3", "3"
 "TRANSPORT_RETRY_DELAY", "producer.transport-retry-delay", "A pause to wait between retrials", "Number, seconds > 0", "5", "5"
 "**MASTER_BOOTSTRAP_MODE**", "producer.master-bootstrap-mode", "It defines how the producer will act during the bootstrap and later regarding of acquiring mastership for related partitions.", "Consts.MASTER_BOOTSTRAP_MODE_FULL, LAZY, LAZY_VOTE", "LAZY", "FULL"
 "Consts.{MASTER_BOOTSTRAP_MODE_FULL, MASTER_BOOTSTRAP_MODE_LAZY, MASTER_BOOTSTRAP_MODE_LAZY_VOTE}", "Constants for master partition ownership distribution", "FULL is for aggressive mode when all the spare partitions are taken first and rebalancing algorithm launched next. LAZY is absolutely passive, driven only by errors. LAZY_VOTE is as LAZY but adds rebalancing algorithm launchedВ after the bootstrap.", "", "", ""			
 "Transaction.TTL", "producer.transaction.ttl", "A timeout after which the transaction is considered as broken/stalled/abandoned and must be purged", "Number, seconds between 3 and 120", "6", "30"
 "Transaction.OPEN_MAXWAIT", "producer.transaction.open-maxwait", "An amount of time producer will wait for the master to open transaction. If theВ master hasn’t opened up to the timeout then the exception will be raised.", "Number, seconds between 1 and 10", "5", "5"
 "Transaction.KEEP_ALIVE", "producer.transaction.keep-alive", "The interval of time the producer sends keep alive notifications for long-lasting transactions", "Number, seconds between 1 and 2", "1", "1"
 "Transaction.DATA_WRITE_BATCH_SIZE", "producer.transaction.data-write-batch-size", "For better productivity data items inside transactions are written in batches, the parameter specifies how big batches are.", "Number, between 1 and 1000", "200", "1"
 "Transaction.DISTRIBUTION_POLICY", "producer.transaction.distribution-policy", "If a transaction is opened without specific partition request then distribution policy is used to get next partition. See Transacton.Consts.DISTRIBUTION_POLICY_*", "", "", "" 			
 "Transaction.Consts.DISTRIBUTION_POLICY_RR", "round-robin", "Currently only one distribution policy exists – round robin", "", "", "" 


TSF_Dictionary.Consumer keyset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The keyset describes options of consumer and subscriber behavior.	

.. csv-table:: 
 :header: "Key", "Textual constant", "Purpose", "Domain", "Example", "Default"
 :widths: 10, 15, 35, 20, 20, 10

 "**TRANSACTION_PRELOAD**", "consumer.transaction-preload", "Amount of transactions for preloading when it reads from Metadata storage", "Number between 1 and 100", "10", "10"
 "**DATA_PRELOAD**", "consumer.data-preload", "Amount of data items for preloading when it reads from Data storage", "Number between 100 and 200", "200", "100"
 "**Subscriber.BIND_HOST**", "consumer.subscriber.bind-host", "Host address to bind to", "String ipv4 or hostname", "1.1.1.1", "localhost"
 "**Subscriber.BIND_PORT**", "consumer.subscriber.bind-port", "Host port to bind to", "Number or (from,to)", "(18000, 20000)", "18001"
 "**Subscriber.PERSISTENT_QUEUE_PATH**", "consumer.subscriber.persistent-queue.path", "The path of the queue that stores ready but not yet handled transactions", "Valid path or null", "/tmp/consumer-1", "/tmp"
 "**Subscriber.TRANSACTION_BUFFER_THREAD_POOL**", "consumer.subscriber.transaction-buffer-thread-pool", "Amount of threads that are used for managing transactions buffers", "Number between 1 and 64", "16", "4"
 "**Subscriber.PROCESSING_ENGINES_THREAD_POOL**", "consumer.subscriber.processing-engines-thread-pool", "Amount of threads that are used for managing ready transactions", "Number between 1 and 64", "16", "1"
 "**Subscriber.POLLING_FREQUENCY_DELAY**", "consumer.subscriber.polling-frequency-delay", "An Interval that specifies how often polling should be launched if no new events.", "Number, milliseconds between 100 and 100000", "1000", "1000"
	
