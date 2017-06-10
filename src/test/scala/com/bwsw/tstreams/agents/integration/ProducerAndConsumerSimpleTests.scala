package com.bwsw.tstreams.agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.{Newest, Oldest}
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.testutils._
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


class ProducerAndConsumerSimpleTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  lazy val srv = TestStorageServer.getNewClean()

  lazy val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0))

  lazy val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0),
    offset = Oldest,
    useLastOffset = true)



  override def beforeAll(): Unit = {
    srv
    createNewStream()
    consumer.start()
  }


  "producer, consumer" should "producer - generate one transaction, consumer - retrieve it with getAll method" in {
    val DATA_IN_TRANSACTION = 10

    val producerTransaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    val l = new CountDownLatch(1)
    srv.notifyProducerTransactionCompleted(t => t.transactionID == producerTransaction.getTransactionID && t.state == TransactionStates.Checkpointed, l.countDown())
    val sendData = (for (part <- 0 until DATA_IN_TRANSACTION) yield "data_part_" + randomKeyspace).sorted
    sendData.foreach { x =>
      producerTransaction.send(x.getBytes())
    }
    producerTransaction.checkpoint()
    l.await()

    val transaction = consumer.getTransaction(0).get

    transaction.getAll.map(i => new String(i)).sorted shouldBe sendData

    //assert that is nothing to read
    (0 until consumer.stream.partitionsCount) foreach { _ =>
      consumer.getTransaction(0).isEmpty shouldBe true
    }
  }

  "producer, consumer" should "producer - generate one transaction, consumer - retrieve it using iterator" in {
    val DATA_IN_TRANSACTION = 10
    val sendData = (for (part <- 0 until DATA_IN_TRANSACTION) yield "data_part_" + randomKeyspace).sorted

    val producerTransaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    val l = new CountDownLatch(1)
    srv.notifyProducerTransactionCompleted(t => t.transactionID == producerTransaction.getTransactionID && t.state == TransactionStates.Checkpointed, l.countDown())

    sendData.foreach { x => producerTransaction.send(x.getBytes()) }
    producerTransaction.checkpoint()
    l.await()

    val transactionOpt = consumer.getTransaction(0)
    transactionOpt.isDefined shouldBe true

    val transaction = transactionOpt.get

    var readData = ListBuffer[String]()

    while (transaction.hasNext) {
      val s = new String(transaction.next())
      readData += s
    }

    readData.toList.sorted shouldBe sendData

    //assert that is nothing to read
    (0 until consumer.stream.partitionsCount) foreach { _ => consumer.getTransaction(0).isEmpty shouldBe true }

  }

  "producer, consumer" should "producer - generate some set of transactions, consumer - retrieve them all" in {
    val TRANSACTIONS_COUNT = 100
    val DATA_IN_TRANSACTION = 10

    val sendData = (for (part <- 0 until DATA_IN_TRANSACTION) yield "data_part_" + randomKeyspace).sorted

    val l = new CountDownLatch(1)

    var counter = 0

    (0 until TRANSACTIONS_COUNT).foreach { _ =>
      val producerTransaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

      counter += 1
      if (counter == TRANSACTIONS_COUNT)
        srv.notifyProducerTransactionCompleted(t => t.transactionID == producerTransaction.getTransactionID && t.state == TransactionStates.Checkpointed, l.countDown())

      sendData.foreach { x => producerTransaction.send(x.getBytes()) }
      producerTransaction.checkpoint()
    }

    l.await()

    (0 until TRANSACTIONS_COUNT).foreach { _ =>
      val transaction = consumer.getTransaction(0)
      transaction.nonEmpty shouldBe true
      transaction.get.getAll.map(i => new String(i)).sorted == sendData
    }

    //assert that is nothing to read
    (0 until consumer.stream.partitionsCount) foreach { _ =>
      consumer.getTransaction(0).isEmpty shouldBe true
    }
  }

  "producer, consumer" should "producer - generate some set of transactions after cancel, consumer - retrieve them all" in {
    val TRANSACTIONS_COUNT = 100
    val DATA_IN_TRANSACTION = 1

    val pl = ListBuffer[Long]()
    val cl = ListBuffer[Long]()

    val sendData = (for (part <- 0 until DATA_IN_TRANSACTION) yield "data_part_" + randomKeyspace).sorted

    val l = new CountDownLatch(1)

    var counter = 0

    val producerTransaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    producerTransaction.cancel()

    (0 until TRANSACTIONS_COUNT).foreach { _ =>
      val producerTransaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

      pl.append(producerTransaction.getTransactionID)

      counter += 1
      if (counter == TRANSACTIONS_COUNT)
        srv.notifyProducerTransactionCompleted(t => t.transactionID == producerTransaction.getTransactionID && t.state == TransactionStates.Checkpointed, l.countDown())

      sendData.foreach { x => producerTransaction.send(x.getBytes()) }
      producerTransaction.checkpoint()
    }

    l.await()
    (0 until TRANSACTIONS_COUNT).foreach { i =>
      val transactionOpt = consumer.getTransaction(0)
      transactionOpt.nonEmpty shouldBe true
      cl.append(transactionOpt.get.getTransactionID)
    }

    cl shouldBe pl

  }

  "producer, consumer" should "producer - generate transaction, consumer retrieve it (both start async)" in {
    val timeoutForWaiting = 5
    val DATA_IN_TRANSACTION = 10

    val sendData = (for (part <- 0 until DATA_IN_TRANSACTION) yield "data_part_" + part).sorted

    val producerThread = new Thread(() => {
      val transaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
      Thread.sleep(100)
      sendData.foreach { x =>
        transaction.send(x.getBytes())
      }
      transaction.checkpoint()
    })

    val consumerThread = new Thread(() => {
      breakable {
        while (true) {
          val transactionOpt = consumer.getTransaction(0)
          if (transactionOpt.isDefined) {
            transactionOpt.get.getAll.map(i => new String(i)).sorted shouldBe sendData
            break()
          }
          Thread.sleep(100)
        }
      }
    })

    producerThread.start()
    consumerThread.start()
    producerThread.join(timeoutForWaiting * 1000)
    consumerThread.join(timeoutForWaiting * 1000)

    consumerThread.isAlive shouldBe false
    producerThread.isAlive shouldBe false

    (0 until consumer.stream.partitionsCount) foreach { _ => consumer.getTransaction(0).isEmpty shouldBe true }
  }

  "producer and consumer" should "work correctly for instant transactions" in {
    val consumer = f.getConsumer(
      name = "test_consumer",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = false)

    val transactionID1 = producer.instantTransaction(0, Seq("test1".getBytes), isReliable = true)
    val transactionID2 = producer.instantTransaction(0, Seq("test2".getBytes), isReliable = false)

    consumer.start()
    consumer.getTransactionById(0, transactionID1)
      .foreach(transaction => {
        transaction.getTransactionID shouldBe transactionID1
        new String(transaction.next()) shouldBe "test1"
      })

    consumer.getTransactionById(0, transactionID2)
      .foreach(transaction =>  {
        transaction.getTransactionID shouldBe transactionID2
        new String(transaction.next()) shouldBe "test2"
      })

    consumer.stop()
  }

  override def afterAll(): Unit = {
    producer.stop()
    consumer.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}

