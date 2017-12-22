package it

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.{RocksReader, RocksWriter}
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, TransactionStates}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils.startZkServerAndGetIt

class SingleNodeServerClientInterconnectionLifecycleTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

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


  it should "put stream, then delete this stream, and put it again and return correct result" in {
    val bundle = util.Utils.getRocksReaderAndRocksWriter(zkClient)

    val streamService = bundle.streamService

    val stream =
      com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, None, 100L)

    val streamAfterDelete =
      com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, Some("Previous one was deleted"), 538L)


    streamService.putStream(
      stream.name,
      stream.partitions,
      stream.description,
      stream.ttl
    )

    streamService.delStream(stream.name)

    val streamAfterDeleteWithID = streamService.putStream(
      streamAfterDelete.name,
      streamAfterDelete.partitions,
      streamAfterDelete.description,
      streamAfterDelete.ttl
    )

    val retrievedStream = streamService.getStream(streamAfterDelete.name).get

    com.bwsw.tstreamstransactionserver.rpc.Stream(
      streamAfterDeleteWithID,
      streamAfterDelete.name,
      streamAfterDelete.partitions,
      streamAfterDelete.description,
      streamAfterDelete.ttl,
      s"${bundle.storageOptions.streamZookeeperDirectory}/ids/id0000000001"
    ) shouldBe retrievedStream

    bundle.closeDBAndDeleteFolder()
  }

  private final def getProducerTransactionFromServer(rocksReader: RocksReader,
                                                     producerTransaction: ProducerTransaction) = {
    rocksReader.scanTransactions(
      producerTransaction.stream,
      producerTransaction.partition,
      producerTransaction.transactionID,
      producerTransaction.transactionID,
      Int.MaxValue,
      Set()
    ).producerTransactions.head
  }


  private final def transitOneTransactionToAnotherState(rocksReader: RocksReader,
                                                        rocksWriter: RocksWriter,
                                                        in: ProducerTransaction,
                                                        toUpdateIn: ProducerTransaction,
                                                        out: ProducerTransaction,
                                                        timeBetweenTransactionMs: Long) = {
    val firstCommitTime = System.currentTimeMillis()
    val inAggregated = ProducerTransactionRecord(
      in,
      firstCommitTime
    )

    val batch1 = rocksWriter.getNewBatch
    rocksWriter.putTransactions(Seq(inAggregated), batch1)
    batch1.write()

    val secondCommitTime = System.currentTimeMillis()
    val toUpdateInAggregated = ProducerTransactionRecord(
      toUpdateIn,
      secondCommitTime + timeBetweenTransactionMs
    )

    val batch2 = rocksWriter.getNewBatch
    rocksWriter.putTransactions(Seq(toUpdateInAggregated), batch2)
    batch2.write()

    getProducerTransactionFromServer(rocksReader, out) shouldBe out
  }


  it should "put stream, then put producerTransaction with states in following order: Opened->Checkpointed. Should return Checkpointed Transaction" in {
    val bundle = util.Utils.getRocksReaderAndRocksWriter(zkClient)

    val streamService = bundle.streamService
    val rocksReader = bundle.rocksReader
    val rocksWriter = bundle.rocksWriter


    val stream = com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, None, 100L)
    val streamID = streamService.putStream(stream.name, stream.partitions, stream.description, stream.ttl)


    val openedTTL = TimeUnit.SECONDS.toMillis(2)
    val checkpointedTTL = TimeUnit.SECONDS.toMillis(3)
    val producerTransaction = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      streamID, stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL
    )

    val producerTransactionCheckpointed = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTransaction.stream, producerTransaction.partition,
      producerTransaction.transactionID,
      TransactionStates.Checkpointed, -1,
      checkpointedTTL
    )

    transitOneTransactionToAnotherState(
      rocksReader,
      rocksWriter,
      producerTransaction,
      producerTransactionCheckpointed,
      producerTransactionCheckpointed,
      openedTTL - TimeUnit.SECONDS.toMillis(1)
    )

    bundle.closeDBAndDeleteFolder()
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Checkpointed. Should return Invalid Transaction(due to expiration)" in {
    val bundle = util.Utils.getRocksReaderAndRocksWriter(zkClient)

    val streamService = bundle.streamService
    val rocksReader = bundle.rocksReader
    val rocksWriter = bundle.rocksWriter

    val stream = com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, None, 100L)
    val streamID = streamService.putStream(stream.name, stream.partitions, stream.description, stream.ttl)


    val openedTTL = TimeUnit.SECONDS.toMillis(4)
    val checkpointedTTL = TimeUnit.SECONDS.toMillis(2)
    val producerTransaction = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      streamID,
      stream.partitions,
      System.currentTimeMillis(),
      TransactionStates.Opened,
      -1,
      openedTTL
    )

    val producerTransactionCheckpointed = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTransaction.stream,
      producerTransaction.partition,
      producerTransaction.transactionID,
      TransactionStates.Checkpointed,
      -1,
      checkpointedTTL
    )

    transitOneTransactionToAnotherState(
      rocksReader,
      rocksWriter,
      producerTransaction,
      producerTransactionCheckpointed,
      producerTransaction.copy(state = TransactionStates.Invalid, quantity = 0, ttl = 0L),
      openedTTL + TimeUnit.SECONDS.toMillis(1)
    )

    bundle.closeDBAndDeleteFolder()
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Updated->Updated->Checkpointed. Should return Checkpointed Transaction" in {
    val bundle = util.Utils.getRocksReaderAndRocksWriter(zkClient)

    val streamService = bundle.streamService
    val rocksReader = bundle.rocksReader
    val rocksWriter = bundle.rocksWriter

    val stream = com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, None, 100L)
    val streamID = streamService.putStream(stream.name, stream.partitions, stream.description, stream.ttl)

    val openedTTL = TimeUnit.SECONDS.toMillis(7L)
    val updatedTTL1 = openedTTL
    val producerTxnOpened = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      streamID,
      stream.partitions,
      System.currentTimeMillis(),
      TransactionStates.Opened,
      -1,
      openedTTL
    )

    val producerTxnUpdated1 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTxnOpened.stream,
      producerTxnOpened.partition,
      producerTxnOpened.transactionID,
      TransactionStates.Updated,
      -1,
      updatedTTL1
    )

    transitOneTransactionToAnotherState(rocksReader, rocksWriter,
      producerTxnOpened,
      producerTxnUpdated1,
      producerTxnOpened,
      openedTTL - TimeUnit.SECONDS.toMillis(2)
    )

    val updatedTTL2 = openedTTL
    val producerTxnUpdated2 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTxnOpened.stream,
      producerTxnOpened.partition,
      producerTxnOpened.transactionID,
      TransactionStates.Updated,
      -1,
      updatedTTL2

    )
    transitOneTransactionToAnotherState(rocksReader, rocksWriter,
      producerTxnOpened,
      producerTxnUpdated2,
      producerTxnOpened,
      updatedTTL2 - TimeUnit.SECONDS.toMillis(2)
    )

    val updatedTTL3 = openedTTL
    val producerTxnUpdated3 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTxnOpened.stream,
      producerTxnOpened.partition,
      producerTxnOpened.transactionID,
      TransactionStates.Updated,
      -1,
      updatedTTL3
    )
    transitOneTransactionToAnotherState(rocksReader, rocksWriter,
      producerTxnOpened,
      producerTxnUpdated3,
      producerTxnOpened,
      updatedTTL3 - TimeUnit.SECONDS.toMillis(2)
    )

    val checkpointedTTL = TimeUnit.SECONDS.toMillis(6)
    val producerTransactionCheckpointed = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTxnOpened.stream,
      producerTxnOpened.partition,
      producerTxnOpened.transactionID,
      TransactionStates.Checkpointed,
      -1,
      checkpointedTTL
    )

    transitOneTransactionToAnotherState(rocksReader, rocksWriter,
      producerTxnOpened,
      producerTransactionCheckpointed,
      producerTransactionCheckpointed,
      checkpointedTTL - TimeUnit.SECONDS.toMillis(2)
    )

    bundle.closeDBAndDeleteFolder()
  }


  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Updated->Updated->Checkpointed. Should return Invalid Transaction(due to expiration)" in {
    val bundle = util.Utils.getRocksReaderAndRocksWriter(zkClient)

    val streamService = bundle.streamService
    val rocksReader = bundle.rocksReader
    val rocksWriter = bundle.rocksWriter

    val stream = com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, None, 100L)
    val streamID = streamService.putStream(stream.name, stream.partitions, stream.description, stream.ttl)


    val openedTTL = TimeUnit.SECONDS.toMillis(7L)
    val updatedTTL1 = TimeUnit.SECONDS.toMillis(5L)
    val wait1 = openedTTL - TimeUnit.SECONDS.toMillis(1)
    val producerTxnOpened = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      streamID,
      stream.partitions,
      System.currentTimeMillis(),
      TransactionStates.Opened,
      -1,
      openedTTL
    )

    val producerTxnUpdated1 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTxnOpened.stream,
      producerTxnOpened.partition,
      producerTxnOpened.transactionID,
      TransactionStates.Updated,
      -1,
      updatedTTL1
    )

    transitOneTransactionToAnotherState(rocksReader, rocksWriter,
      producerTxnOpened,
      producerTxnUpdated1,
      producerTxnOpened.copy(ttl = updatedTTL1),
      wait1
    )

    val updatedTTL2 = TimeUnit.SECONDS.toMillis(2L)
    val wait2 = updatedTTL2 - TimeUnit.SECONDS.toMillis(2)
    val producerTxnUpdated2 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTxnOpened.stream,
      producerTxnOpened.partition,
      producerTxnOpened.transactionID,
      TransactionStates.Updated,
      -1,
      updatedTTL2
    )

    transitOneTransactionToAnotherState(rocksReader, rocksWriter,
      producerTxnOpened,
      producerTxnUpdated2,
      producerTxnOpened.copy(ttl = updatedTTL2),
      wait2
    )

    val updatedTTL3 = TimeUnit.SECONDS.toMillis(7L)
    val wait3 = updatedTTL3 - TimeUnit.SECONDS.toMillis(2)
    val producerTxnUpdated3 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTxnOpened.stream,
      producerTxnOpened.partition,
      producerTxnOpened.transactionID,
      TransactionStates.Updated,
      -1,
      updatedTTL3
    )
    transitOneTransactionToAnotherState(rocksReader, rocksWriter,
      producerTxnOpened,
      producerTxnUpdated3,
      producerTxnOpened.copy(state = TransactionStates.Invalid, quantity = 0, ttl = 0L),
      wait3
    )

    val checkpointedTTL = TimeUnit.SECONDS.toMillis(2L)
    val wait4 = checkpointedTTL - TimeUnit.SECONDS.toMillis(2)
    val producerTransactionCheckpointed = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTxnOpened.stream,
      producerTxnOpened.partition,
      producerTxnOpened.transactionID,
      TransactionStates.Checkpointed, -1,
      checkpointedTTL)
    transitOneTransactionToAnotherState(rocksReader, rocksWriter,
      producerTxnOpened,
      producerTransactionCheckpointed,
      producerTxnOpened.copy(state = TransactionStates.Invalid, quantity = 0, ttl = 0L),
      wait4
    )

    bundle.closeDBAndDeleteFolder()
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Cancel->Updated->Checkpointed. Should return Invalid Transaction(due to transaction with Cancel state)" in {
    val bundle = util.Utils.getRocksReaderAndRocksWriter(zkClient)

    val streamService = bundle.streamService
    val rocksReader = bundle.rocksReader
    val rocksWriter = bundle.rocksWriter

    val stream = com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, None, 100L)
    val streamID = streamService.putStream(stream.name, stream.partitions, stream.description, stream.ttl)


    val openedTTL = TimeUnit.SECONDS.toMillis(7L)
    val updatedTTL1 = TimeUnit.SECONDS.toMillis(4L)
    val wait1 = openedTTL - TimeUnit.SECONDS.toMillis(1)
    val producerTxnOpened = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      streamID,
      stream.partitions,
      System.currentTimeMillis(),
      TransactionStates.Opened,
      -1,
      openedTTL
    )
    val producerTxnUpdated1 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTxnOpened.stream,
      producerTxnOpened.partition,
      producerTxnOpened.transactionID,
      TransactionStates.Updated,
      -1,
      updatedTTL1
    )

    transitOneTransactionToAnotherState(rocksReader, rocksWriter,
      producerTxnOpened,
      producerTxnUpdated1,
      producerTxnOpened.copy(ttl = updatedTTL1),
      wait1
    )

    val updatedTTL2 = TimeUnit.SECONDS.toMillis(1L)
    val wait2 = TimeUnit.SECONDS.toMillis(1L)
    val producerTxnCancel2 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTxnOpened.stream,
      producerTxnOpened.partition,
      producerTxnOpened.transactionID,
      TransactionStates.Cancel,
      -1,
      updatedTTL2
    )
    transitOneTransactionToAnotherState(rocksReader, rocksWriter,
      producerTxnOpened.copy(ttl = updatedTTL1),
      producerTxnCancel2,
      producerTxnOpened.copy(state = TransactionStates.Invalid, quantity = 0, ttl = 0L),
      wait2
    )

    val updatedTTL3 = TimeUnit.SECONDS.toMillis(7L)
    val wait3 = updatedTTL3 - TimeUnit.SECONDS.toMillis(2)
    val producerTxnUpdated3 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTxnOpened.stream,
      producerTxnOpened.partition,
      producerTxnOpened.transactionID,
      TransactionStates.Updated,
      -1,
      updatedTTL3
    )
    transitOneTransactionToAnotherState(rocksReader, rocksWriter,
      producerTxnOpened,
      producerTxnUpdated3,
      producerTxnOpened.copy(state = TransactionStates.Invalid, quantity = 0, ttl = 0L),
      wait3
    )

    val checkpointedTTL = TimeUnit.SECONDS.toMillis(2L)
    val wait4 = checkpointedTTL - TimeUnit.SECONDS.toMillis(2)
    val producerTransactionCheckpointed = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTxnOpened.stream,
      producerTxnOpened.partition,
      producerTxnOpened.transactionID,
      TransactionStates.Checkpointed,
      -1,
      checkpointedTTL
    )
    transitOneTransactionToAnotherState(rocksReader, rocksWriter,
      producerTxnOpened,
      producerTransactionCheckpointed,
      producerTxnOpened.copy(state = TransactionStates.Invalid, quantity = 0, ttl = 0L),
      wait4
    )

    bundle.closeDBAndDeleteFolder()
  }
}
