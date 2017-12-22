package it


import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionKey, ProducerTransactionRecord}
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, TransactionStates}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils._


class SingleNodeServerProducerTransactionsCleanerTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private val rand = scala.util.Random

  private def getRandomStream = com.bwsw.tstreamstransactionserver.rpc.StreamValue(
    name = rand.nextInt(10000).toString,
    partitions = rand.nextInt(10000),
    description = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None,
    ttl = Long.MaxValue
  )

  private val txnCounter = new AtomicLong(0)
  private def getRandomProducerTransaction(streamID: Int,
                                           streamObj: com.bwsw.tstreamstransactionserver.rpc.StreamValue,
                                           ttlTxn: Long) = ProducerTransaction(
    stream = streamID,
    partition = streamObj.partitions,
    transactionID = txnCounter.getAndIncrement(),
    state = TransactionStates.Opened,
    quantity = -1,
    ttl = ttlTxn
  )

  private lazy val (zkServer, zkClient) =
    startZkServerAndGetIt

  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }

  "Cleaner" should "remove all expired transactions from OpenedTransactions table and invalidate them in AllTransactions table" in {
    val bundle = util.Utils.getRocksReaderAndRocksWriter(zkClient)

    val streamService = bundle.streamService
    val rocksReader = bundle.rocksReader
    val rocksWriter = bundle.rocksWriter

    val maxTTLForProducerTransactionSec = 5
    val producerTxnNumber = 100

    def ttlSec = TimeUnit.SECONDS.toMillis(rand.nextInt(maxTTLForProducerTransactionSec))

    val stream = getRandomStream

    val streamID = streamService.putStream(stream.name, stream.partitions, stream.description, stream.ttl)

    val currentTime = System.currentTimeMillis()
    val producerTransactionsWithTimestamp: Array[(ProducerTransaction, Long)] = Array.fill(producerTxnNumber) {
      val producerTransaction = getRandomProducerTransaction(streamID, stream, ttlSec)
      (producerTransaction, System.currentTimeMillis())
    }
    val minTransactionID = producerTransactionsWithTimestamp.minBy(_._1.transactionID)._1.transactionID
    val maxTransactionID = producerTransactionsWithTimestamp.maxBy(_._1.transactionID)._1.transactionID

    val transactionsWithTimestamp = producerTransactionsWithTimestamp.map {
      case (producerTxn, timestamp) => ProducerTransactionRecord(producerTxn, timestamp)
    }

    val batch = bundle.newBatch
    rocksWriter.putTransactions(transactionsWithTimestamp, batch)
    batch.write()

    rocksWriter.createAndExecuteTransactionsToDeleteTask(
      currentTime + TimeUnit.SECONDS.toMillis(maxTTLForProducerTransactionSec)
    )

    val expiredTransactions = producerTransactionsWithTimestamp.map { case (producerTxn, _) =>
      ProducerTransaction(
        producerTxn.stream,
        producerTxn.partition,
        producerTxn.transactionID,
        TransactionStates.Invalid,
        0,
        0L
      )
    }

    rocksReader.scanTransactions(
      streamID,
      stream.partitions,
      minTransactionID,
      maxTransactionID,
      Int.MaxValue,
      Set(TransactionStates.Opened)
    ).producerTransactions should contain theSameElementsAs expiredTransactions

    bundle.closeDBAndDeleteFolder()
  }

}
