Storage Client
=====================

Instance Constructor::

 new StorageClient(clientOptions: ConnectionOptions, authOptions: AuthOptions, zookeeperOptions: ZookeeperOptions, curator: CuratorFramework, tracingOptions: TracingOptions = TracingOptions())
 
Methods to Add
-------------------

def authenticationKey: String

def checkConsumerOffsetExists(consumerName: String, streamID: Int,partition: Int, timeout: Duration = StorageClient.maxAwaiTimeout):Boolean - Check if a consumer exists.

def checkStreamExists(streamName: String, timeout: Duration =StorageClient.maxAwaiTimeout): Boolean - Check if a stream exists.

def createStream(streamName: String, partitionsCount: Int, ttl: Long,description: String, timeout: Duration = StorageClient.maxAwaiTimeout):Stream - Allows creating a stream.

def curatorClient: CuratorFramework

def deleteStream(streamName: String, timeout: Duration =StorageClient.maxAwaiTimeout): Boolean - Allows deleting a stream.

def generateTransaction(timeout: Duration = StorageClient.maxAwaiTimeout):Long

def generateTransactionForTimestamp(time: Long, timeout: Duration =StorageClient.maxAwaiTimeout): Long

def getCommitLogOffsets(timeout: Duration = StorageClient.maxAwaiTimeout):CommitLogInfo

def getLastSavedConsumerOffset(consumerName: String, streamID: Int,partition: Int, timeout: Duration = StorageClient.maxAwaiTimeout): Long - Allows retrieving a specific offset for a particular consumer.

def getLastTransactionId(streamID: Int, partition: Integer, timeout:Duration = StorageClient.maxAwaiTimeout): Long

def getTransaction(streamID: Int, partition: Integer, transactionID: Long,timeout: Duration = StorageClient.maxAwaiTimeout):Option[ProducerTransaction]

def getTransactionData(streamID: Int, partition: Int, transaction: Long,from: Int, to: Int, timeout: Duration = StorageClient.maxAwaiTimeout):Seq[Array[Byte]]

def isConnected: Boolean

val isShutdown: AtomicBoolean

def loadStream(streamName: String, timeout: Duration =StorageClient.maxAwaiTimeout): Stream - Allows getting an existing stream.

def openTransactionSync(streamID: Int, partition: Int, transactionTtlMs:Long, timeout: Duration = StorageClient.maxAwaiTimeout): Long

def putInstantTransactionSync(streamID: Int, partition: Int, data:Seq[Array[Byte]], timeout: Duration = StorageClient.maxAwaiTimeout):Long

def putInstantTransactionUnreliable(streamID: Int, partition: Int, data:Seq[Array[Byte]]): Unit

def putTransactionData(streamID: Int, partition: Int, transaction: Long,data: Seq[Array[Byte]], from: Int): Future[Boolean]

def putTransactionSync(transaction: ProducerTransaction, timeout: Duration= StorageClient.maxAwaiTimeout): Boolean

def putTransactionWithDataSync[T](transaction: ProducerTransaction, data:Seq[Array[Byte]], lastOffset: Int, timeout: Duration =StorageClient.maxAwaiTimeout): Boolean

def putTransactions(producerTransactions: Seq[ProducerTransaction],consumerTransactions: Seq[ConsumerTransaction], timeout: Duration =StorageClient.maxAwaiTimeout): Boolean 

def saveConsumerOffset(consumerName: String, streamID: Int, partition: Int,offset: Long, timeout: Duration = StorageClient.maxAwaiTimeout): Unit - Allows saving a single offset.

def saveConsumerOffsetBatch(consumerName: String, streamID: Int,partitionAndLastTransaction: Map[Int, Long], timeout: Duration =StorageClient.maxAwaiTimeout): Boolean - Allows saving an offset batch.

def scanTransactions(streamID: Int, partition: Integer, from: Long, to:Long, count: Int, states: Set[TransactionStates], timeout: Duration =StorageClient.maxAwaiTimeout): (Long, Seq[ProducerTransaction])

def shutdown(): Unit
