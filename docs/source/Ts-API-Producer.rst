Producer API
==================
The Producer object is used to create new transactions, fill them with data and send those transactions to a persistent storage and to Subscribers (if any exist). Also, those transactions may be retrieved in the future by Consumers.

.. Contents::

API of a producer.Producer
-------------------------------

Basically, from developer's perspective, a Producer performs the following sequence of operations:

1) it creates a new transaction using the ``newTransaction`` method
2) it sends data to the transaction using the send method
3) it fixes the transaction using the checkpoint or cancel method

The sequence above shows explicit operation behavior, but the 3rd step can be implicit and we will see it later.

The creation of a producer should be done with the ``getProducer`` method of TStreamsFactory_. See TStreamsFactory_ manual for details of how to construct a Producer object.

newTransaction method
~~~~~~~~~~~~~~~~~~~~~~~~

*(updated)*

After the producer object is created next step is to create a new transaction *(need updating)*::
	
 val t = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)

The ``newTransaction`` method has the following signature::
	
 def newTransaction(policy: ProducerPolicy = ..., partition: Int = -1): ProducerTransactionImpl

It returns a new transaction object and takes up to two parameters:

.. csv-table:: 
 :header: "Parameter", "Purpose", "Example"
 :widths: 10, 55, 25


 "policy", "Specifies the `policy <https://github.com/bwsw/t-streams/blob/develop/t-streams/src/main/scala/com/bwsw/tstreams/agents/producer/NewProducerTransactionPolicy.scala>`_ to apply to the previously open (not checkpointed or canceled transaction), four policies are available (which are applied in the case when the previous transaction is still opened):

 1. **CheckpointIfOpened** – when the transaction is opened the previous one is checkpointed synchronously.
 2. **CheckpointAsyncIfOpened** – when the transaction is opened the previous one is checkpointed asynchronously (without waiting).
 3. **CancelIfOpened** – when the transaction is opened the previous one is canceled.
 4. **ErrorIfOpened** – when the transaction is opened, the exception is raised.", "policy = NewTransactionProducerPolicy.CheckpointIfOpened"
 "partition", "specifies the partition of the stream on which the transaction will be created; if the argument is omitted then next partition (according to the policy that was specified in getProducer factory method) will be used.", "0"
.. "retry", "defines a number of retrials if the method fails internally (it happens when partition master is gone away during the call of newTransaction).", "3"

The ``producer.Transaction`` object API will be described further.

So, the method above starts a new transaction. That transaction will move through the next states:

.. figure:: _static/API_Producer_TxnStatuses.png

*Opened* and *Updated* states are internal for a developer and are displayed for better understanding. Finally, after a new transaction is received it will move to *checkpoint* or to *cancel*.

A producer has an API which allows to do *checkpoint* or *cancel* for all opened transactions (every stream partition can have one opened transaction simultaneously) *(?)*.

The ``newTransaction`` method can throw MaterializationException *(obsolete?)* exception, which means that partition master has gone away during the operation and the operation is unable to succeed. For more explanation see the figure below.

.. figure:: _static/materialization-1.png

Materialization allows starting the operation with the transaction as soon as possible while master *(?)* still does required background stuff. So, basically, wrap ``newTransaction`` inside of the **try … catch** block to avoid crashes.

checkpoint method
~~~~~~~~~~~~~~~~~~~~~~~

There is a method for checkpoint::

 def checkpoint(partition: Int): Producer

which closes all opened transactions for producer partitions. Keep in mind that the checkpoint is not atomic. Atomic checkpoints are achieved with CheckpointGroup API *(?)*.

An example of the usage is shown below::

 producer.checkpoint()

*(?)* In general, you use a synchronous checkpoint if you would like to do the next operations only after all of the opened transactions are checkpointed for sure, otherwise you can use an asynchronous variant which offers better performance::
	
 producer.checkpoint(isAsynchronous = true)

*(?)* Keep in mind that if the ``newTransaction`` method uses the ``CheckpointIfOpened`` or ``CheckpointAsyncIfOpened`` policies then an opened transaction will be checkpointed automatically when the ``newTransaction`` method will be called for the partition where there is an opened transaction. But, of course You can still checkpoint them explicitly.

One more method is for ... (need more information)::

 def checkpoint(): Producer

cancel method
~~~~~~~~~~~~~~~~~~~

Sometimes a logic of a program determines some kind that current opened transactions are invalid and would like to cancel them. In this case the cancel method must be used, which terminates all the opened transactions switching them to the cancel state.

::

 def cancel(partition: Int): Option[(Long,Set[ProducerTransaction])]

An example of the usage is shown below *(need updating)*::
	
 producer.cancel()

*(?)* Keep in mind that if the ``newTransaction`` method uses the ``CancelIfOpened`` policy then an opened transaction will be cancelled automatically when the ``newTransaction`` method will be called for the partition where there is an opened transaction. But, of course you can still cancel them explicitly.

getOpenedTransactionForPartition method
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*(obsolete?)*

Often it is not a convinient way to use external variables or containers to keep pointers to opened transactions which have been received with the ``newTransaction`` method. If your processing involves a lot of simultaneously opened transactions (e.g. you use a partition value as a hash key with a hash function like hash(data) -> partition), then you probably would like use the ``getOpenedTransactionForPartition`` method::
	
 def getOpenedTransactionForPartition(partition: Int): Option[IProducerTransaction[T]]

An example of the usage is shown below::
	
 val txn = producer.getOpenedTransactionForPartition(partitionDistributionFun(data))
txn.send(data)

isMasterOfPartition method
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*(obsolete?)*

The method allows to determine either the producer is a master for the partition or not. It is usefull for testing, validation and integration purposes.

::
	
 def isMasterOfPartition(partition: Int): Boolean

stop method
~~~~~~~~~~~~~~~~

*(updated)*

In the end of the operation a producer have to be stopped gracefully. Use the stop method.

::
	
 def stop(): Unit

An example of the usage is shown below *(need updating)*::

 producer.stop()

Now the producer is no longer functional.

Methods to add
~~~~~~~~~~~~~~~~~~~

def close(): Unit 

def generateNewTransaction(partition: Int,isInstant: Boolean = false, isReliable: Boolean= true, data: Seq[Array[Byte]] = Seq()): Long

def instantTransaction(data: Seq[Array[Byte]],isReliable: Boolean): Long

def instantTransaction(partition: Int, data:Seq[Array[Byte]], isReliable: Boolean): Long

def isConnected: Boolean

var name: String

val producerOptions: ProducerOptions

def publish(msg: TransactionState): Unit

val stream: Stream

API of a producer.Transaction
-------------------------------

*(updated)*

A producer.Transaction object has some important methods which allows a developer to effectively manipulate with it. They are presented in the table below:

.. csv-table:: 
 :header: "Method", "Purpose"
 :widths: 55, 55

 "def isClosed: Boolean", "Returns True if the transaction is no longer fit for usage"
 "def getPartition: Int", "Returns the partition on that the transaction operates"
 "def toString(): String", "Returns a string presentation of the transaction"
 "def getTransactionID: Long", "Returns the ID of the transaction"
 "def getDataItemsCount: Int", "Returns the amount of data items inside the transaction"
 "def getProducer: Producer[T]", "Returns the Producer instance that created the transaction"
 "def send(string: String): ProducerTransaction", "Allows to send a new data item into the transaction"
 "def send(obj: Array[Byte]): ProducerTransaction", "Allows to send data to storage"
 "def cancel(): Unit", "Allows to cancel the transaction"
 "def checkpoint(): Unit", "Allows to checkpoint the transaction"
 "def finalizeDataSend(): Unit", "Does actual sending of data that is not sent yet"
 "def getStateInfo(checkpoint: Boolean):ProducerTransactionState", ""
 "def markAsClosed(): Unit", ""

Most of the methods above are self explaining. Let’s take a look at the last three ones.

send method
~~~~~~~~~~~~~~~~

The method allows one to send a data item into a transaction. The data item is put into internal transaction buffer, and when the buffer reaches its limit all the data items are sent into the datastore. When the data item is sent to the transaction it is no longer possible to “undo” it.

cancel method
~~~~~~~~~~~~~~~~~~

Previously, we have already seen the cancel method for Producer object. It cancels all of the opened transactions. The transaction is also able to call the cancel method which cancels only that transaction.

checkpoint method
~~~~~~~~~~~~~~~~~~~~~~~

The last one method is checkpoint. It has the same semantics and meaning as the checkpoint method of Producer object, but it is related to the transaction, not for all of the opened tranasactions. It also can be ether synchronous or asynchronous. *(?)*



