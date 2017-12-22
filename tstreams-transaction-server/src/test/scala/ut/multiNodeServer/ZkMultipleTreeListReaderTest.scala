package ut.multiNodeServer

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.{LongNodeCache, LongZookeeperTreeList, ZkMultipleTreeListReader}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.LedgerManager
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, TimestampRecord}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{IsOkayStatus, LedgerMetadata, MoveToNextLedgerStatus}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.storage.BookkeeperWrapper
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.{Checkpointed, Opened}
import com.bwsw.tstreamstransactionserver.rpc._
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import ut.multiNodeServer.ZkTreeListTest.LedgerManagerInMemory
import util.Utils

class ZkMultipleTreeListReaderTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {
  private val rand = scala.util.Random
  private val streamIDGen = new java.util.concurrent.atomic.AtomicInteger(0)
  private val partitionsNumber = 100

  private val ensembleNumber = 4
  private val writeQourumNumber = 3
  private val ackQuorumNumber = 2

  private val bookkeeperOptions = BookkeeperOptions(
    ensembleNumber,
    writeQourumNumber,
    ackQuorumNumber,
    "test".getBytes()
  )

  private val bookiesNumber =
    ensembleNumber max writeQourumNumber max ackQuorumNumber

  private lazy val (zkServer, zkClient, bookies) =
    Utils.startZkServerBookieServerZkClient(bookiesNumber)

  private lazy val bookKeeper: BookKeeper = {
    val lowLevelZkClient = zkClient.getZookeeperClient
    val configuration = new ClientConfiguration()
      .setZkServers(
        lowLevelZkClient.getCurrentConnectionString
      )
      .setZkTimeout(lowLevelZkClient.getConnectionTimeoutMs)

    configuration.setLedgerManagerFactoryClass(
      classOf[LongHierarchicalLedgerManagerFactory]
    )

    new BookKeeper(configuration)
  }


  private def generateStream =
    Stream(
      id = streamIDGen.getAndIncrement(),
      name = rand.nextString(10),
      partitions = partitionsNumber,
      None,
      Long.MaxValue,
      ""
    )

  private def getRandomProducerTransaction(streamID: Int,
                                           partition: Int,
                                           state: TransactionStates,
                                           txnID: Long,
                                           ttlTxn: Long) =
    ProducerTransaction(
      stream = streamID,
      partition = partition,
      transactionID = txnID,
      state = state,
      quantity = -1,
      ttl = ttlTxn
    )

  private def uuid = java.util.UUID.randomUUID.toString


  override def afterAll(): Unit = {
    bookies.foreach(_.shutdown())
    zkClient.close()
    zkServer.close()
  }


  "ZkMultipleTreeListReader" should "not retrieve records from database ZkTreeListLong objects don't have entities" in {
    val storage = new LedgerManagerInMemory

    val zkTreeList1 = new LongZookeeperTreeList(zkClient, s"/$uuid")
    val zkTreeList2 = new LongZookeeperTreeList(zkClient, s"/$uuid")


    val zkTreeListLastClosedLedgerPrefix1 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix1,
        Array.emptyByteArray
      )

    val zkTreeListLastClosedLedgerPrefix2 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix2,
        Array.emptyByteArray
      )

    val commonMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix1
      )

    val checkpointMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix2
      )

    val lastClosedLedgerHandlers =
      Array(commonMasterLastClosedLedger, checkpointMasterLastClosedLedger)
    lastClosedLedgerHandlers.foreach(_.startMonitor())


    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      lastClosedLedgerHandlers,
      storage
    )

    val ledgerRecordIDs = Array.empty[LedgerMetadata]
    val (records, updatedLedgersWithTheirLastRecords) =
      testReader.read(ledgerRecordIDs)

    records shouldBe empty
    ledgerRecordIDs should contain theSameElementsAs updatedLedgersWithTheirLastRecords
  }


  it should "not retrieve records as one of ZkTreeListLong objects doesn't have entities" in {
    val storage = new LedgerManagerInMemory

    val firstLedger = storage.createLedger(System.currentTimeMillis())

    val zkTreeList1 = new LongZookeeperTreeList(zkClient, s"/$uuid")
    val zkTreeList2 = new LongZookeeperTreeList(zkClient, s"/$uuid")

    zkTreeList2.createNode(firstLedger.id)

    val zkTreeListLastClosedLedgerPrefix1 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix1,
        Array.emptyByteArray
      )

    val zkTreeListLastClosedLedgerPrefix2 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix2,
        java.nio.ByteBuffer.allocate(8).putLong(firstLedger.id).array()
      )

    val commonMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix1
      )

    val checkpointMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix2
      )

    val lastClosedLedgerHandlers =
      Array(commonMasterLastClosedLedger, checkpointMasterLastClosedLedger)
    lastClosedLedgerHandlers.foreach(_.startMonitor())


    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      lastClosedLedgerHandlers,
      storage
    )

    val ledgerRecordIDs = Array.empty[LedgerMetadata]
    val (records, updatedLedgersWithTheirLastRecords) =
      testReader.read(ledgerRecordIDs)

    records shouldBe empty
    ledgerRecordIDs should contain theSameElementsAs updatedLedgersWithTheirLastRecords
  }


  private def test1(storage: LedgerManager) = {
    val stream = generateStream

    val producerTransactionsNumber = 50
    val initialTime = 0L
    val firstTimestamp = initialTime
    val atomicLong = new AtomicLong(initialTime)

    val firstTreeRecords = {
      (0 until producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Opened,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            Frame.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }

    atomicLong
      .set(initialTime)
    val secondTimestamp =
      atomicLong.get()


    val secondTreeRecords = {
      (0 until producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Checkpointed,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            Frame.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }

    val firstLedger = storage.createLedger(firstTimestamp)
    firstTreeRecords.foreach(record => firstLedger.addRecord(record))
    firstLedger.close()

    val secondLedger = storage.createLedger(secondTimestamp)
    secondTreeRecords.foreach(record => secondLedger.addRecord(record))
    secondLedger.close()

    val zkTreeList1 = new LongZookeeperTreeList(zkClient, s"/$uuid")
    val zkTreeList2 = new LongZookeeperTreeList(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)

    val zkTreeListLastClosedLedgerPrefix1 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix1,
        java.nio.ByteBuffer.allocate(8).putLong(firstLedger.id).array()
      )

    val zkTreeListLastClosedLedgerPrefix2 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix2,
        java.nio.ByteBuffer.allocate(8).putLong(secondLedger.id).array()
      )

    val commonMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix1
      )

    val checkpointMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix2
      )

    val lastClosedLedgerHandlers =
      Array(commonMasterLastClosedLedger, checkpointMasterLastClosedLedger)
    lastClosedLedgerHandlers.foreach(_.startMonitor())


    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      lastClosedLedgerHandlers,
      storage
    )

    val (records1, updatedLedgersWithTheirLastRecords1) =
      testReader.read(Array.empty[LedgerMetadata])

    val (records2, updatedLedgersWithTheirLastRecords2) =
      testReader.read(updatedLedgersWithTheirLastRecords1)

    records1.length shouldBe producerTransactionsNumber * 2 + 2
    records2 shouldBe empty

    updatedLedgersWithTheirLastRecords1.head shouldBe
      LedgerMetadata(
        ledgerID = firstLedger.id,
        ledgerLastRecordID = 49,
        MoveToNextLedgerStatus
      )

    updatedLedgersWithTheirLastRecords1.tail.head shouldBe
      LedgerMetadata(
        ledgerID = secondLedger.id,
        ledgerLastRecordID = 49,
        MoveToNextLedgerStatus
      )

    updatedLedgersWithTheirLastRecords1 should contain theSameElementsInOrderAs updatedLedgersWithTheirLastRecords2
  }

  it should "retrieve records from database because ZkTreeListLong objects called 'treeList1' and 'treeList2' have ledgers ids and a storage contains records within the ledgers," +
    " ledgers are closed at the same time" in {
    val bookKeeperStorage = new BookkeeperWrapper(
      bookKeeper,
      bookkeeperOptions
    )

    val storage = new LedgerManagerInMemory

    test1(storage)
    test1(bookKeeperStorage)
  }


  private def test2(storage: LedgerManager) = {
    val stream = generateStream

    val producerTransactionsNumber = 99

    val initialTime = 0L
    val atomicLong = new AtomicLong(initialTime)
    val firstTimestamp =
      atomicLong.getAndIncrement()

    val firstTreeRecords = {
      (0 until producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Opened,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            Frame.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }


    atomicLong.set(initialTime + 50)
    val secondTimestamp =
      atomicLong.getAndIncrement()


    val secondTreeRecords = {
      (0 until producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Checkpointed,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            Frame.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }


    val firstLedger = storage.createLedger(firstTimestamp)
    firstTreeRecords.foreach(record => firstLedger.addRecord(record))
    firstLedger.close()

    val secondLedger = storage.createLedger(secondTimestamp)
    secondTreeRecords.foreach(record => secondLedger.addRecord(record))
    secondLedger.close()

    val zkTreeList1 = new LongZookeeperTreeList(zkClient, s"/$uuid")
    val zkTreeList2 = new LongZookeeperTreeList(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)

    val zkTreeListLastClosedLedgerPrefix1 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix1,
        java.nio.ByteBuffer.allocate(8).putLong(firstLedger.id).array()
      )

    val zkTreeListLastClosedLedgerPrefix2 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix2,
        java.nio.ByteBuffer.allocate(8).putLong(secondLedger.id).array()
      )

    val commonMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix1
      )

    val checkpointMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix2
      )

    val lastClosedLedgerHandlers =
      Array(commonMasterLastClosedLedger, checkpointMasterLastClosedLedger)
    lastClosedLedgerHandlers.foreach(_.startMonitor())


    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      lastClosedLedgerHandlers,
      storage
    )

    val (records1, updatedLedgersWithTheirLastRecords1) =
      testReader.read(Array.empty[LedgerMetadata])

    val (records2, updatedLedgersWithTheirLastRecords2) =
      testReader.read(updatedLedgersWithTheirLastRecords1)

    records1.length shouldBe 150
    records2.length shouldBe 0

    updatedLedgersWithTheirLastRecords1.head shouldBe
      LedgerMetadata(
        ledgerID = firstLedger.id,
        ledgerLastRecordID = 98,
        MoveToNextLedgerStatus
      )

    updatedLedgersWithTheirLastRecords1.tail.head shouldBe
      LedgerMetadata(
        ledgerID = secondLedger.id,
        ledgerLastRecordID = 48,
        IsOkayStatus
      )

    updatedLedgersWithTheirLastRecords1 should contain theSameElementsInOrderAs updatedLedgersWithTheirLastRecords2
  }

  it should "retrieve records from database ZkTreeListLong objects called 'treeList1' and 'treeList2' have ledgers ids and a storage contains records within the ledgers," +
    " first ledger(belongs to 'treeList1') is closed earlier than second ledger(belongs to 'treeList2')" in {
    val bookKeeperStorage = new BookkeeperWrapper(
      bookKeeper,
      bookkeeperOptions
    )

    val storage = new LedgerManagerInMemory

    test2(storage)
    test2(bookKeeperStorage)
  }

  it should "retrieve records from database because ZkTreeListLong objects called 'treeList1' and 'treeList2' have ledgers ids and a storage contains records within the ledgers," +
    " the first ledger(belongs to 'treeList1') is closed earlier than the second ledger(belongs to 'treeList2), and the third ledger(belongs to 'treeList1)" in {
    val stream = generateStream

    val producerTransactionsNumber = 99

    val initialTime = 0L
    val firstTimestamp = initialTime
    val atomicLong = new AtomicLong(initialTime)

    val firstLedgerRecords = {
      (0 to producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Opened,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            Frame.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }


    val firstBarrier = atomicLong.getAndSet(initialTime + 50)
    val secondTimestamp = atomicLong.get()

    val secondLedgerRecords = {
      (0 to producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Checkpointed,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            Frame.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }


    atomicLong.set(firstBarrier + 20)
    val thirdTimestamp =
      atomicLong.get()

    val thirdLedgerRecords = {
      (0 to producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Checkpointed,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            Frame.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }

    val storage = new LedgerManagerInMemory

    val firstLedger = storage.createLedger(firstTimestamp)
    firstLedgerRecords.foreach(record => firstLedger.addRecord(record))
    firstLedger.close()

    val secondLedger = storage.createLedger(secondTimestamp)
    secondLedgerRecords.foreach(record => secondLedger.addRecord(record))
    secondLedger.close()

    val zkTreeList1 = new LongZookeeperTreeList(zkClient, s"/$uuid")
    val zkTreeList2 = new LongZookeeperTreeList(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)

    val thirdLedger = storage.createLedger(thirdTimestamp)
    thirdLedgerRecords.foreach(record => thirdLedger.addRecord(record))
    thirdLedger.close()

    zkTreeList1.createNode(thirdLedger.id)


    val zkTreeListLastClosedLedgerPrefix1 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix1,
        java.nio.ByteBuffer.allocate(8).putLong(thirdLedger.id).array()
      )

    val zkTreeListLastClosedLedgerPrefix2 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix2,
        java.nio.ByteBuffer.allocate(8).putLong(secondLedger.id).array()
      )

    val commonMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix1
      )

    val checkpointMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix2
      )

    val lastClosedLedgerHandlers =
      Array(commonMasterLastClosedLedger, checkpointMasterLastClosedLedger)
    lastClosedLedgerHandlers.foreach(_.startMonitor())


    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      lastClosedLedgerHandlers,
      storage
    )

    val (records1, updatedLedgersWithTheirLastRecords1) =
      testReader.read(Array.empty[LedgerMetadata])

    val (records2, updatedLedgersWithTheirLastRecords2) =
      testReader.read(updatedLedgersWithTheirLastRecords1)

    records1.length shouldBe 152
    records2.length shouldBe 81

    updatedLedgersWithTheirLastRecords1.head shouldBe
      LedgerMetadata(
        ledgerID = firstLedger.id,
        ledgerLastRecordID = 99,
        MoveToNextLedgerStatus
      )

    updatedLedgersWithTheirLastRecords1.tail.head shouldBe
      LedgerMetadata(
        ledgerID = secondLedger.id,
        ledgerLastRecordID = 49,
        IsOkayStatus
      )

    updatedLedgersWithTheirLastRecords2.head shouldBe
      LedgerMetadata(
        ledgerID = thirdLedger.id,
        ledgerLastRecordID = 29,
        IsOkayStatus
      )

    updatedLedgersWithTheirLastRecords2.tail.head shouldBe
      LedgerMetadata(
        ledgerID = secondLedger.id,
        ledgerLastRecordID = 99,
        MoveToNextLedgerStatus
      )
  }

  it should "retrieve records from database because ZkTreeListLong objects called 'treeList1' and 'treeList2' have ledgers ids and a storage contains records within the ledgers," +
    " second ledger(belongs to 'treeList2') is closed earlier than first ledger(belongs to 'treeList1')" in {
    val stream = generateStream

    val producerTransactionsNumber = 99
    val initialTime = 0L
    val firstTimestamp = initialTime
    val atomicLong = new AtomicLong(initialTime)

    val firstTreeRecords = {
      (0 to producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Opened,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            Frame.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }


    atomicLong
      .set(initialTime - 50)
    val secondTimestamp =
      atomicLong.get()

    val secondTreeRecords = {
      (0 to producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Checkpointed,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            Frame.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }


    val storage = new LedgerManagerInMemory

    val firstLedger = storage.createLedger(firstTimestamp)
    firstTreeRecords.foreach(record => firstLedger.addRecord(record))


    val secondLedger = storage.createLedger(secondTimestamp)
    secondTreeRecords.foreach(record => secondLedger.addRecord(record))

    val zkTreeList1 = new LongZookeeperTreeList(zkClient, s"/$uuid")
    val zkTreeList2 = new LongZookeeperTreeList(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)

    val zkTreeListLastClosedLedgerPrefix1 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix1,
        java.nio.ByteBuffer.allocate(8).putLong(firstLedger.id).array()
      )

    val zkTreeListLastClosedLedgerPrefix2 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix2,
        java.nio.ByteBuffer.allocate(8).putLong(secondLedger.id).array()
      )

    val commonMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix1
      )

    val checkpointMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix2
      )

    val lastClosedLedgerHandlers =
      Array(commonMasterLastClosedLedger, checkpointMasterLastClosedLedger)
    lastClosedLedgerHandlers.foreach(_.startMonitor())


    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      lastClosedLedgerHandlers,
      storage
    )

    val (records1, updatedLedgersWithTheirLastRecords1) =
      testReader.read(Array.empty[LedgerMetadata])

    val (records2, updatedLedgersWithTheirLastRecords2) =
      testReader.read(updatedLedgersWithTheirLastRecords1)

    records1.length shouldBe 152
    records2.length shouldBe 0

    updatedLedgersWithTheirLastRecords1.head shouldBe
      LedgerMetadata(
        ledgerID = firstLedger.id,
        ledgerLastRecordID = 49,
        IsOkayStatus
      )

    updatedLedgersWithTheirLastRecords1.tail.head shouldBe
      LedgerMetadata(
        ledgerID = secondLedger.id,
        ledgerLastRecordID = 99,
        MoveToNextLedgerStatus
      )

    updatedLedgersWithTheirLastRecords1 should contain theSameElementsInOrderAs updatedLedgersWithTheirLastRecords2
  }

  it should "process ledgers records that created earlier than the last one" in {
    val initialTime = 0L
    val atomicLong = new AtomicLong(initialTime)

    val firstTimestamp =
      atomicLong.getAndSet(150L)


    val secondTimestamp =
      atomicLong.getAndSet(300L)


    val thirdTimestamp =
      atomicLong.getAndSet(350L)


    val forthTimestamp =
      atomicLong.getAndSet(400L)


    val storage = new LedgerManagerInMemory

    val firstLedger = storage.createLedger(firstTimestamp)
    firstLedger.close()

    val secondLedger = storage.createLedger(secondTimestamp)
    secondLedger.close()

    val thirdLedger = storage.createLedger(thirdTimestamp)
    thirdLedger.close()

    val forthLedger = storage.createLedger(forthTimestamp)
    forthLedger.close()


    val zkTreeList1 = new LongZookeeperTreeList(zkClient, s"/$uuid")
    val zkTreeList2 = new LongZookeeperTreeList(zkClient, s"/$uuid")


    zkTreeList2.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)
    zkTreeList2.createNode(thirdLedger.id)

    zkTreeList1.createNode(forthLedger.id)

    val zkTreeListLastClosedLedgerPrefix1 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix1,
        java.nio.ByteBuffer.allocate(8).putLong(forthLedger.id).array()
      )

    val zkTreeListLastClosedLedgerPrefix2 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix2,
        java.nio.ByteBuffer.allocate(8).putLong(thirdLedger.id).array()
      )

    val commonMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix1
      )

    val checkpointMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix2
      )

    val lastClosedLedgerHandlers =
      Array(commonMasterLastClosedLedger, checkpointMasterLastClosedLedger)
    lastClosedLedgerHandlers.foreach(_.startMonitor())


    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      lastClosedLedgerHandlers,
      storage
    )

    val (records1, updatedLedgersWithTheirLastRecords1) =
      testReader.read(Array.empty[LedgerMetadata])

    records1.length shouldBe 1
    updatedLedgersWithTheirLastRecords1.head.id shouldBe forthLedger.id
    updatedLedgersWithTheirLastRecords1.head.lastRecordID shouldBe -1
    updatedLedgersWithTheirLastRecords1.tail.head.id shouldBe firstLedger.id
    updatedLedgersWithTheirLastRecords1.tail.head.lastRecordID shouldBe -1L

    val (records2, updatedLedgersWithTheirLastRecords2) =
      testReader.read(updatedLedgersWithTheirLastRecords1)

    records2.length shouldBe 1
    updatedLedgersWithTheirLastRecords2.head.id shouldBe forthLedger.id
    updatedLedgersWithTheirLastRecords2.head.lastRecordID shouldBe -1L
    updatedLedgersWithTheirLastRecords2.tail.head.id shouldBe secondLedger.id
    updatedLedgersWithTheirLastRecords2.tail.head.lastRecordID shouldBe -1L

    val (records3, updatedLedgersWithTheirLastRecords3) =
      testReader.read(updatedLedgersWithTheirLastRecords2)

    records3.length shouldBe 1
    updatedLedgersWithTheirLastRecords3.head.id shouldBe forthLedger.id
    updatedLedgersWithTheirLastRecords3.head.lastRecordID shouldBe -1L
    updatedLedgersWithTheirLastRecords3.tail.head.id shouldBe thirdLedger.id
    updatedLedgersWithTheirLastRecords3.tail.head.lastRecordID shouldBe -1L
  }

}
