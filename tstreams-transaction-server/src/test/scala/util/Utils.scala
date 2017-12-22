package util

import java.io.File
import java.net.ServerSocket
import java.nio.file.Files
import java.util
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.client.api.TTSClient
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.{SingleNodeServerBuilder, TestSingleNodeServer}
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.{RocksReader, RocksWriter, TransactionServer, singleNode}
import com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{RocksStorageOptions, StorageOptions}
import org.apache.bookkeeper.conf.ServerConfiguration
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory
import org.apache.bookkeeper.proto.BookieServer
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.ACL


object Utils {
  def uuid: String = java.util.UUID.randomUUID.toString

  private val sessionTimeoutMillis = 1000
  private val connectionTimeoutMillis = 1000


  def startZkServerAndGetIt: (TestingServer, CuratorFramework) = {
    val zkServer = new TestingServer(true)

    val zkClient = CuratorFrameworkFactory.builder
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(new RetryNTimes(3, 100))
      .connectString(zkServer.getConnectString)
      .build()

    zkClient.start()
    zkClient.blockUntilConnected(connectionTimeoutMillis, TimeUnit.MILLISECONDS)

    (zkServer, zkClient)
  }


  private val zkLedgersRootPath = "/ledgers"
  private val zkBookiesAvailablePath = s"$zkLedgersRootPath/available"

  def startBookieServer(zkEndpoints: String, bookieNumber: Int): BookieServer = {

    def createBookieFolder() = {
      val path = Files.createTempDirectory(s"bookie")

      val bookieFolder =
        new File(path.toFile.getPath, "current")

      bookieFolder.mkdir()

      bookieFolder.getPath
    }

    def startBookie(): BookieServer = {
      val bookieFolder = createBookieFolder()

      val serverConfig = new ServerConfiguration()
        .setBookiePort(Utils.getRandomPort)
        .setZkServers(zkEndpoints)
        .setJournalDirName(bookieFolder)
        .setLedgerDirNames(Array(bookieFolder))
        .setAllowLoopback(true)
        .setJournalFlushWhenQueueEmpty(true)

      serverConfig
        .setZkLedgersRootPath(zkLedgersRootPath)

      serverConfig.setLedgerManagerFactoryClass(
        classOf[LongHierarchicalLedgerManagerFactory]
      )

      val server = new BookieServer(serverConfig)
      server.start()
      server
    }

    startBookie()
  }

  def startZkServerBookieServerZkClient(serverNumber: Int): (TestingServer, CuratorFramework, Array[BookieServer]) = {
    val (zkServer, zkClient) = startZkServerAndGetIt

    zkClient.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .withACL(new util.ArrayList[ACL](Ids.OPEN_ACL_UNSAFE))
      .forPath(zkLedgersRootPath)

    zkClient.create()
      .withMode(CreateMode.PERSISTENT)
      .withACL(new util.ArrayList[ACL](Ids.OPEN_ACL_UNSAFE))
      .forPath(zkBookiesAvailablePath)

    val bookies = (0 until serverNumber).map(serverIndex =>
      startBookieServer(
        zkClient.getZookeeperClient.getCurrentConnectionString,
        serverIndex
      )
    ).toArray

    (zkServer, zkClient, bookies)
  }

  private val rand = scala.util.Random

  def getRandomStream =
    new com.bwsw.tstreamstransactionserver.rpc.StreamValue {
      override val name: String = rand.nextInt(10000).toString
      override val partitions: Int = rand.nextInt(10000)
      override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
      override val ttl: Long = Long.MaxValue
      override val zkPath: Option[String] = None
    }

  def getRandomPort: Int = {
    scala.util.Try {
      new ServerSocket(0)
    }.map { server =>
      val port = server.getLocalPort
      server.close()
      port
    }.get
  }

  private def testStorageOptions(dbPath: File) = {
    StorageOptions().copy(
      path = dbPath.getPath,
      streamZookeeperDirectory = s"/$uuid"
    )
  }

  private def tempFolder() = {
    Files.createTempDirectory("tts").toFile
  }

  def getRocksReaderAndRocksWriter(zkClient: CuratorFramework) = {
    val dbPath = tempFolder()

    val storageOptions =
      testStorageOptions(dbPath)

    val rocksStorageOptions =
      RocksStorageOptions()


    new RocksReaderAndWriter(zkClient, storageOptions, rocksStorageOptions)
  }

  def getTransactionServerBundle(zkClient: CuratorFramework): TransactionServerBundle = {
    val dbPath = tempFolder()

    val storageOptions =
      testStorageOptions(dbPath)

    val rocksStorageOptions =
      RocksStorageOptions()

    val rocksStorage =
      new MultiAndSingleNodeRockStorage(
        storageOptions,
        rocksStorageOptions
      )

    val zkStreamRepository =
      new ZookeeperStreamRepository(
        zkClient,
        storageOptions.streamZookeeperDirectory
      )

    val transactionDataService =
      new TransactionDataService(
        storageOptions,
        rocksStorageOptions,
        zkStreamRepository
      )

    val rocksWriter =
      new RocksWriter(
        rocksStorage,
        transactionDataService
      )

    val rocksReader =
      new RocksReader(
        rocksStorage,
        transactionDataService
      )

    val transactionServer =
      new TransactionServer(
        zkStreamRepository,
        rocksWriter,
        rocksReader
      )

    val oneNodeCommitLogService =
      new singleNode.commitLogService.CommitLogService(
        rocksStorage.getStorageManager
      )

    new TransactionServerBundle(
      transactionServer,
      oneNodeCommitLogService,
      rocksStorage,
      transactionDataService,
      storageOptions,
      rocksStorageOptions
    )
  }

  def startTransactionServer(builder: SingleNodeServerBuilder): ZkSeverAndTransactionServer = {
    val zkTestServer = new TestingServer(true)
    val transactionServer = builder
      .withZookeeperOptions(
        builder.getZookeeperOptions.copy(endpoints = zkTestServer.getConnectString)
      )
      .withBootstrapOptions(
        builder.getBootstrapOptions.copy(bindPort = getRandomPort)
      )
      .build()

    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()

    latch.await(3000, TimeUnit.SECONDS)
    ZkSeverAndTransactionServer(zkTestServer, transactionServer)
  }

  def startTransactionServerAndClient(zkClient: CuratorFramework,
                                      serverBuilder: SingleNodeServerBuilder,
                                      clientBuilder: ClientBuilder): ZkSeverTxnServerTxnClient = {
    val dbPath = Files.createTempDirectory("tts").toFile
    val zKCommonMasterPrefix = s"/$uuid"


    val updatedBuilder = serverBuilder
      .withCommonRoleOptions(
        serverBuilder.getCommonRoleOptions.copy(
          commonMasterPrefix = zKCommonMasterPrefix,
          commonMasterElectionPrefix = s"/$uuid")
      )
      .withZookeeperOptions(
        serverBuilder.getZookeeperOptions.copy(
          endpoints = zkClient.getZookeeperClient.getCurrentConnectionString
        )
      )
      .withServerStorageOptions(
        serverBuilder.getStorageOptions.copy(
          path = dbPath.getPath,
          streamZookeeperDirectory = s"/$uuid")
      )
      .withBootstrapOptions(
        serverBuilder.getBootstrapOptions.copy(bindPort = getRandomPort)
      )

    val transactionServer = new TestSingleNodeServer(
      updatedBuilder.getAuthenticationOptions,
      updatedBuilder.getZookeeperOptions,
      updatedBuilder.getBootstrapOptions,
      updatedBuilder.getCommonRoleOptions,
      updatedBuilder.getCheckpointGroupRoleOptions,
      updatedBuilder.getStorageOptions,
      updatedBuilder.getRocksStorageOptions,
      updatedBuilder.getCommitLogOptions,
      updatedBuilder.getPackageTransmissionOptions,
      updatedBuilder.getSubscribersUpdateOptions,
      updatedBuilder.getTracingOptions
    )

    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()

    if (!latch.await(5000, TimeUnit.SECONDS))
      throw new IllegalStateException()

    val client = new ClientBuilder()
      .withConnectionOptions(ConnectionOptions(prefix = zKCommonMasterPrefix))
      .withZookeeperOptions(
        ZookeeperOptions(
          endpoints = zkClient.getZookeeperClient.getCurrentConnectionString
        )
      )
      .build()

    new ZkSeverTxnServerTxnClient(transactionServer, client, updatedBuilder)
  }

  def startTransactionServerAndClient(zkClient: CuratorFramework,
                                      serverBuilder: SingleNodeServerBuilder,
                                      clientBuilder: ClientBuilder,
                                      clientsNumber: Int): ZkSeverTxnServerTxnClients = {
    val dbPath = Files.createTempDirectory("tts").toFile

    val streamRepositoryPath =
      s"/$uuid"

    val zkConnectionString =
      zkClient.getZookeeperClient.getCurrentConnectionString

    val port =
      getRandomPort

    val updatedBuilder = serverBuilder
      .withZookeeperOptions(
        serverBuilder.getZookeeperOptions.copy(
          endpoints = zkConnectionString
        )
      )
      .withServerStorageOptions(
        serverBuilder.getStorageOptions.copy(
          path = dbPath.getPath,
          streamZookeeperDirectory = streamRepositoryPath)
      )
      .withBootstrapOptions(
        serverBuilder.getBootstrapOptions.copy(bindPort = port)
      )


    val transactionServer = new TestSingleNodeServer(
      updatedBuilder.getAuthenticationOptions,
      updatedBuilder.getZookeeperOptions,
      updatedBuilder.getBootstrapOptions,
      updatedBuilder.getCommonRoleOptions,
      updatedBuilder.getCheckpointGroupRoleOptions,
      updatedBuilder.getStorageOptions,
      updatedBuilder.getRocksStorageOptions,
      updatedBuilder.getCommitLogOptions,
      updatedBuilder.getPackageTransmissionOptions,
      updatedBuilder.getSubscribersUpdateOptions,
      updatedBuilder.getTracingOptions
    )

    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()

    if (!latch.await(5000, TimeUnit.SECONDS))
      throw new IllegalStateException()


    val clients: Array[TTSClient] = Array.fill(clientsNumber) {
      new ClientBuilder()
        .withZookeeperOptions(
          ZookeeperOptions(
            endpoints = zkConnectionString
          )
        ).build()
    }

    new ZkSeverTxnServerTxnClients(transactionServer, clients, updatedBuilder)
  }


}
