Subscriber API
==================

A Subscriber is the common accessor interface (?) in T-streams. 

We already know a Consumer which can be used in more specific access patterns when a developer wishes to use T-streams like time-series database. 

A Subscriber operates in a PUB-SUB mode. It means that a developer does not poll the metadata store for new transactions, but a Subscriber receives updates about new events naturally as they come. To handle those transactions a Subscriber allows registering a Callback class instance where a developer has to override the **onEvent** method which is called every time the Subscriber receives a completed transaction.

Keep in mind, that the transactions are correctly ordered only inside of every partition, but for two different partitions – it’s not true. You can’t rely on that (transaction1, partition1) and (transaction2, partition2) are ordered.

Also, don’t remember that since the **onTransaction** method is called for different partitions and can be called in parallel a developer should use a synchronization (this.synchronized or a mutex) primitives If there are shared objects exist. The snippet below gives an overview of a Callback trait a developer should implement::
	
 package com.bwsw.tstreams.agents.consumer.subscriber
 
 import com.bwsw.tstreams.agents.consumer.{Consumer, Transaction, TransactionOperator}
 
 /**
   * Trait to implement to handle incoming messages
   */
 trait Callback[T] {
   /**
     * Callback which is called on every closed transaction
     *
     * @param consumer        associated Consumer
     * @param transaction
     */
   def onTransaction(consumer: TransactionOperator[T],
               transaction: Transaction[T]): Unit
 
   ...
 }

Let’s review **onTransaction** method closely::

 def onEvent(consumer: TransactionOperator[T], transaction: Transaction[T]): Unit

The first argument is the consumer which is presented with a ``TransactionOperator[T]`` trait. Actually, a developer can cast it to a Consumer instance if he or she wishes to use additional methods that are absent in ``TransactionOperator``. In most of the situations, a developer does not need to use this argument.

The second argument is the ready-to-use transaction which **onEvent** logic will handle. See a Transaction (link to Consumer API here) reference for details.

Take a look at example from t-streams-hello below to understand how to use it::
	
    val l = new CountDownLatch(1)
    var cntr = 0
    var sum = 0L
 
    val subscriber = factory.getSubscriber[String](
      name          = "test_subscriber",              // name of the subscribing consumer
      txnGenerator  = new LocalTransactionGenerator,     // where it can get transaction ids
      converter     = new ArrayByteToStringConverter, // vice versa converter to string
      partitions    = Setup.PARTS.toSet,                        // active partitions
      offset        = Newest,                         // it will start from newest available partitions
      isUseLastOffset = false,                        // will ignore history
      callback = new Callback[String] {
        override def onEvent(op: TransactionOperator[String], txn: Transaction[String]): Unit = this.synchronized {
          txn.getAll().foreach(i => sum += Integer.parseInt(i))                           // get all information from transaction
          cntr += 1
          if (cntr % 100 == 0) {
            println(cntr)
            op.checkpoint()
          }
          if(cntr == Setup.TOTAL_TXNS)                                              // if the producer sent all information, then end
            l.countDown()
        }
      })
 
    subscriber.start() // start subscriber to operate
    l.await()
    subscriber.stop() // stop operation

So, that’s it. You don’t need other methods or tricks to handle transactions. But, let’s take a look at **start** and **stop** methods.

start method
-----------------------

The method starts the operation. It is very important to call for a start in the moment a developer really would like to start operation, not immediately after the instance creation (but, in most of the real-life cases it has no difference, hopefully).

stop method
-------------------

The method stops the operation. A developer calls for it when there is no further activity.

Methods To Add
--------------------

def calculateProcessingEngineWorkersThreadAmount():Int

def close(): Unit

def dumpCounters(): Unit

def getConsumer: Consumer

def isConnected: Boolean

val callback: Callback

val name: String

val options: SubscriberOptions

val stream: Stream

val transactionsBuffers: Map[Int,TransactionBuffer]
