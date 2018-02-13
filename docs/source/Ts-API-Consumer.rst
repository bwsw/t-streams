Consumer API
==================

.. Contents::

There are two reasons to use the Consumer API:

1. To get access to low-level querying methods which provide functions allowing a developer to work with stored transactions in a time-series database style. 

2. To implement a custom polling process that is very specific and is not covered by the standard flow provided by the Subscriber API.

Two types of methods exist – those which change consumer offsets and influence on checkpoint state change and those which do not.

Let’s introduce the Consumer offsets conception. A Consumer does stream processing from a certain offset (see a TStreamsFactory_ page for details). The **start** method is very critical in that respect. When a developer gets a new Consumer instance it is not actually functioning yet. To begin a function the developer calls the **start** method. The **start** method reads the current disposition from the metadata store. As a result, when it is started it keeps the initial state inside. 

(image)

Next, the disposition can be changed with one of two methods:

- **def setStreamPartitionOffset(partition: Int,offset: Long): Unit** which is a utility method and often is used with a Subscriber;
- **def getTransaction(partition: Int):Option[ConsumerTransaction]** which is used with Consumer objects to fetch transactions from the database in a natural (time-ordered) way.

And it is fixed with the **checkpoint** method when a developer decides to fix the state to recover the state after a failure or stop. Other methods do not change disposition. 

The name of the consumer is a very important parameter. The name uniquely determines the disposition. Storage server stores the current state of every (consumer-name, stream, partition) which means that if a consumer will be terminated it will start operation from the same place where it was stopped (from the last checkpoint). *(?)*

The summary of methods of a Consumer is presented below:


.. csv-table:: 
 :header: "Method", "Description"
 :widths: 55, 55
 
 "def start(): Consumer", "Starts the operation of a Consumer"
 "def stop(): Unit", "Stops the operation of a Consumer"
 "def getAgentName(): String", "Returns a name of a Consumer"
 "def getPartitions(): Set[Int]", "Returns a set of partitions"
 "def getCurrentOffset(partition: Int): Long", "Returns a read pointer position for the partition"
 "def getTransaction(partition: Int):Option[ConsumerTransaction]", "returns the next transaction according to a AbstractPolicy"
 "def getLastTransaction(partition: Int):Option[ConsumerTransaction]", "Returns the last available transaction for the partition"
 "def getTransactionsFromTo(partition: Int, from:Long, to: Long):ListBuffer[ConsumerTransaction]", "Returns transactions which reside between from and to for the partition"
 "def getTransactionById(partition: Int,transactionID: Long):Option[ConsumerTransaction]", "Returns a transaction by the transactionID for the partition"
 "def setStreamPartitionOffset(partition: Int,offset: Long): Unit", "Sets an offset to the one specified by the Offset for the partition"
 "def loadTransactionFromDB(partition: Int,transactionID: Long):Option[ConsumerTransaction]", "Loads a transaction info from the metadata store"
 "def checkpoint(): Unit", "Does a checkpoint for current read disposition"
 "def buildTransactionObject(partition: Int,transactionID: Long, state: TransactionStates,count: Int): Option[ConsumerTransaction]", "Allows to build Transaction without accessing DB"
 "def getPartitions: Set[Int]", "Returns partitions"
 "def close(): Unit", ""
 "def isConnected: Boolean", ""
 "def getProposedTransactionId: Long", ""
 "def getStateAndClear(): Array[State]", ""
 "def getStorageClient(): StorageClient", ""
 "val name: String", ""
 "val options: ConsumerOptions", ""
 "val stream: Stream", ""
 
start method
-----------------

*(updated)*

The method starts the operation. It is very important to call for a start in the moment a developer really would like to start operation, not immediately after the instance creation (but, in most of the real-life cases it has no difference).

::

 def start(): Consumer

stop method
-------------------

*(updated)*

The method stops the operation. A developer calls for it when there is no further activity.

::

 def stop(): Unit

getTransaction method
------------------------

*(updated)*

The method is the one that should be used to fetch transactions from the partition in a time-based manner. A developer has to specify the partition from that to read a transaction.

After one or more calls of the **getTransaction** method a developer usually calls for the checkpoint method to fix the state.

*(need updating here)*::
	
 val t = consumer.getTransaction(part)
 ...
 consumer.checkpoint()

getCurrentOffset method
-----------------------------

The method is normally used to receive the last transaction that was read from the partition.

*(need updating here)*::
	
 val transactionID = consumer.getCurrentOffset(part)
 val transactionOpt = consumer.getTransactionById(part, transactionID)

getLastTransaction method
------------------------------

*(updated)*

The method is normally used to receive the last complete transaction for the partition. Under the word “complete” it is meant that it is in the checkpointed state. A developer can use the method to implement a polling mechanism (see also **TransactionComparator** class).

*(need updating here)*::
	
 val txnOpt1 = consumer.getLastTransaction(part)
 Thread.sleep(10000)
 val txnOpt2 = consumer.getLastTransaction(part)
 ...

getTransactionsFromTo method
---------------------------------

*(updated)*

The method loads the list of transactions which reside in the interval. The main thing about this method is that it loads transactions only up to the first incomplete one.

*(need updating here)*::
	
 val txns = consumer.getTransactionsFromTo(part, transactionFrom, transactionTo)

getTransactionById method
------------------------------

*(updated)*

The method loads the transaction with the ID from the metadata store. It returns ``Option[Transaction[T]]`` which is None if:

1. there is no such transaction
2. the transaction is still incomplete

*(need updating here)*::

 val txnOpt = consumer.getTransactionById(part, transactionID)

There is also the **loadTransactionFromDB** method which does not return None if the transaction is incomplete.

loadTransactionFromDB method
----------------------------------

*(updated)*

The method loads the transaction with the ID from the metadata store. It returns ``Option[Transaction[T]]`` which is None if there is no such transaction.

*(need updating here)*::
	
 val txnOpt = consumer.loadTransactionFromDB(part, transactionID)

There is also the **getTransactionById** method which also returns None if the transaction is incomplete.

setStreamPartitionOffset method
---------------------------------------
*(updated)*

The method allows setting the offset for the stream. A developer uses the method to change the offset for the partition where next checkpoint will occur.

*(need updating here)*::

 consumer.setStreamPartitionOffset(part, offset)

Keep in mind, that ID might not correspond to an existing transaction, but could be just timestamp-generated. This allows fixing the checkpoint on a specific date/time. Such a pattern is displayed on the snippet below::
	
 ...
 val gen = com.bwsw.tstreams.generator.LocalTransactionGenerator()
 val id = gen.getTransaction(System.currentTimeMillis())
 consumer.setStreamPartitionOffset(part, id)
 consumer.checkpoint()

checkpoint method
---------------------

*(updated)*

The method is described in the top of the page. It allows fixing the current position of the Consumer. The method is atomic and fixes all the partition positions (which are not fixed yet).


getAgentName method
-------------------------------

::

 def getAgentName(): String

getPartitions method
---------------------------------

::

 def getPartitions(): Set[Int]


Consumer Transaction API
===========================


Methods To Add
----------------------

def attach(c: Consumer): Unit

def consumer: Consumer

def getAll: Queue[Array[Byte]]

def getCount: Int

def getPartition: Int

def getState: TransactionStates

def getTTL: Long

def getTransactionID: Long

def hasNext: Boolean

def next(): Array[Byte]

def replay(): Unit

def toString(): String
