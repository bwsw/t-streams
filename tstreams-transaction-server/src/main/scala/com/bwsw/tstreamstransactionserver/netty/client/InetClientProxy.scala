package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.bwsw.tstreamstransactionserver.ExecutionContextGrid
import com.bwsw.tstreamstransactionserver.`implicit`.Implicits
import com.bwsw.tstreamstransactionserver.configProperties.ClientExecutionContextGrid
import com.bwsw.tstreamstransactionserver.exception.Throwable
import com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown
import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.client.api.TTSInetClient
import com.bwsw.tstreamstransactionserver.netty.client.zk.ZKClient
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.{TracingOptions, ZookeeperOptions}
import com.bwsw.tstreamstransactionserver.rpc._
import io.netty.buffer.ByteBuf
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup}
import io.netty.channel.nio.NioEventLoopGroup
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.retry.RetryForever
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}


class InetClientProxy(connectionOptions: ConnectionOptions,
                      authOpts: AuthOptions,
                      zookeeperOptions: ZookeeperOptions,
                      onZKConnectionStateChangedFunc: ConnectionState => Unit = _ => {},
                      onServerConnectionLostFunc: => Unit = {},
                      onRequestTimeoutFunc: => Unit = {},
                      externalCuratorClient: Option[CuratorFramework] = None,
                      tracingOptions: TracingOptions = TracingOptions())
  extends TTSInetClient {

  private final val executionContext =
    new ClientExecutionContextGrid(connectionOptions.threadPool)
  private final val context =
    executionContext.context

  private final val processTransactionsPutOperationPool =
    ExecutionContextGrid("ClientTransactionPool-%d")
  private final val contextForProducerTransactions =
  //    context
    processTransactionsPutOperationPool.getContext


  private final val requestIDGen = new AtomicLong(1L)
  private val logger =
    LoggerFactory.getLogger(this.getClass)
  private val isZKClientExternal =
    externalCuratorClient.isDefined
  private val isShutdown =
    new AtomicBoolean(false)
  private val workerGroup: EventLoopGroup =
    if (Epoll.isAvailable) {
      new EpollEventLoopGroup(
        connectionOptions.threadPool
      )
    }
    else {
      new NioEventLoopGroup(
        connectionOptions.threadPool
      )
    }

  private final val requestIdToResponseCommonMap =
    new ConcurrentHashMap[Long, Promise[ByteBuf]](
      15000,
      0.8f,
      connectionOptions.threadPool
    )

  private final val requestIdToResponseCheckpointGroupMap =
    new ConcurrentHashMap[Long, Promise[ByteBuf]](
      15000,
      0.8f,
      connectionOptions.threadPool
    )
  private val zkConnection =
    externalCuratorClient.getOrElse {
      new ZKClient(
        zookeeperOptions.endpoints,
        zookeeperOptions.sessionTimeoutMs,
        zookeeperOptions.connectionTimeoutMs,
        new RetryForever(zookeeperOptions.retryDelayMs),
        connectionOptions.prefix
      ).client
    }
  private val commonInetClient =
    new InetClient(
      zookeeperOptions,
      connectionOptions,
      authOpts,
      onServerConnectionLost(),
      onRequestTimeout(),
      onZKConnectionStateChanged,
      workerGroup,
      isShutdown.get(),
      zkConnection,
      requestIDGen,
      requestIdToResponseCommonMap,
      context,
      tracingOptions
    )

  private val checkpointGroupInetClient: Future[InetClient] = commonInetClient
    .getZKCheckpointGroupServerPrefix()
    .map { prefix =>
      val connectionOpts =
        connectionOptions.copy(prefix = prefix)

      new InetClient(
        zookeeperOptions,
        connectionOpts,
        authOpts,
        onServerConnectionLost(),
        onRequestTimeout(),
        onZKConnectionStateChangedFunc,
        workerGroup,
        isShutdown.get(),
        zkConnection,
        requestIDGen,
        requestIdToResponseCheckpointGroupMap,
        context,
        tracingOptions
      )
    }(context)


  /** Retrieving an offset between last processed commit log file and current constructed commit log file where a server writes data.
    *
    * @return Future of getCommitLogOffsets operation that can be completed or not. If it is completed it returns:
    *         1)Thrift Struct [[com.bwsw.tstreamstransactionserver.rpc.CommitLogInfo]] which contains:
    *         currentProcessedCommitLog   - the one which is currently relayed with background worker
    *         currentConstructedCommitLog - the one which is currently under write routine
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream object has size in bytes more than defined by a server.
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdown,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path.
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getCommitLogOffsets(): Future[com.bwsw.tstreamstransactionserver.rpc.CommitLogInfo] = {
    if (logger.isDebugEnabled()) logger.debug(s"Calling method getCommitLogOffsets to get offsets.")
    onShutdownThrowException()
    commonInetClient.method[TransactionService.GetCommitLogOffsets.Args, TransactionService.GetCommitLogOffsets.Result, com.bwsw.tstreamstransactionserver.rpc.CommitLogInfo](
      Protocol.GetCommitLogOffsets,
      TransactionService.GetCommitLogOffsets.Args(),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  private def onShutdownThrowException(): Unit =
    if (isShutdown.get()) throw ClientIllegalOperationAfterShutdown

  /** Putting a stream on a server by primitive type parameters.
    *
    * @param stream      a name of stream.
    * @param partitions  a number of stream partitions.
    * @param description a description of stream.
    * @return Future of putStream operation that can be completed or not. If it is completed it returns:
    *         1) ID if stream is persisted by a server or -1 if there is a stream with such name on the server;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdown,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path.
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         5) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    *
    */
  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): Future[Int] = {
    if (logger.isDebugEnabled()) logger.debug(s"Putting stream $stream with $partitions partitions, ttl $ttl and description.")
    onShutdownThrowException()
    commonInetClient.method[TransactionService.PutStream.Args, TransactionService.PutStream.Result, Int](
      Protocol.PutStream,
      TransactionService.PutStream.Args(stream, partitions, description, ttl),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** Putting a stream on a server by Thrift Stream structure.
    *
    * @param stream an object of Thrift Stream [[com.bwsw.tstreamstransactionserver.rpc.StreamValue]] structure.
    * @return Future of putStream operation that can be completed or not. If it is completed it returns:
    *         1) ID if stream is persisted by a server or -1 if there is a stream with such name on the server;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream object has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putStream(stream: com.bwsw.tstreamstransactionserver.rpc.StreamValue): Future[Int] = {
    if (logger.isDebugEnabled()) logger.debug(s"Putting stream ${stream.name} with ${stream.partitions} partitions, ttl ${stream.ttl} and description.")
    onShutdownThrowException()

    commonInetClient.method[TransactionService.PutStream.Args, TransactionService.PutStream.Result, Int](
      Protocol.PutStream,
      TransactionService.PutStream.Args(stream.name, stream.partitions, stream.description, stream.ttl),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** Deleting a stream by name on a server.
    *
    * @param name a name of stream.
    * @return Future of putStream operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if stream is removed by a server or FALSE if a stream is already deleted or there in no such stream on the server;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream name has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def delStream(name: String): Future[Boolean] = {
    if (logger.isDebugEnabled) logger.debug(s"Deleting stream $name.")
    onShutdownThrowException()

    commonInetClient.method[TransactionService.DelStream.Args, TransactionService.DelStream.Result, Boolean](
      Protocol.DelStream,
      TransactionService.DelStream.Args(name),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** Retrieving a stream from a server by it's name.
    *
    * @param name a name of stream.
    * @return Future of getStream operation that can be completed or not. If it is completed it returns:
    *         1) Thrift Stream [[com.bwsw.tstreamstransactionserver.rpc.StreamValue]] if stream  is retrieved from a server or throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]];
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream object has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getStream(name: String): Future[Option[com.bwsw.tstreamstransactionserver.rpc.Stream]] = {
    if (logger.isDebugEnabled()) logger.debug(s"Retrieving stream $name.")
    onShutdownThrowException()

    commonInetClient.method[TransactionService.GetStream.Args, TransactionService.GetStream.Result, Option[com.bwsw.tstreamstransactionserver.rpc.Stream]](
      Protocol.GetStream,
      TransactionService.GetStream.Args(name),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success
    )(context)
  }

  /** Checks by a stream's name that stream saved in database on server.
    *
    * @param name a name of stream.
    * @return Future of checkStreamExists operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if stream is exists in a server database or FALSE if a it's not;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. stream name has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdown,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def checkStreamExists(name: String): Future[Boolean] = {
    if (logger.isInfoEnabled) logger.info(s"Checking stream $name on existence...")
    onShutdownThrowException()

    commonInetClient.method[TransactionService.CheckStreamExists.Args, TransactionService.CheckStreamExists.Result, Boolean](
      Protocol.CheckStreamExists,
      TransactionService.CheckStreamExists.Args(name),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** retrieving transaction id.
    *
    * @return Future of getTransaction operation that can be completed or not. If it is completed it returns:
    *         1) Transaction ID
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdown,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    *
    */
  def getTransaction(): Future[Long] = {
    if (logger.isDebugEnabled())
      logger.debug(s"Retrieving transaction id ...")
    onShutdownThrowException()


    commonInetClient.method[TransactionService.GetTransactionID.Args, TransactionService.GetTransactionID.Result, Long](
      Protocol.GetTransactionID,
      TransactionService.GetTransactionID.Args(),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** retrieving transaction id that is multiplied by timestamp
    *
    * @param timestamp multiplier(100000) to current transaction id.
    * @return Future of getTransaction operation that can be completed or not. If it is completed it returns:
    *         1) Transaction ID
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdown,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    *
    */
  def getTransaction(timestamp: Long): Future[Long] = {
    if (logger.isDebugEnabled)
      logger.debug(s"Retrieving transaction id by timestamp $timestamp ...")
    onShutdownThrowException()


    commonInetClient.method[TransactionService.GetTransactionIDByTimestamp.Args, TransactionService.GetTransactionIDByTimestamp.Result, Long](
      Protocol.GetTransactionIDByTimestamp,
      TransactionService.GetTransactionIDByTimestamp.Args(timestamp),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** Puts producer and consumer transactions on a server.
    *
    * @param producerTransactions some collections of producer transactions [[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]].
    * @param consumerTransactions some collections of consumer transactions [[com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction]].
    * @return Future of putTransactions operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if server put these transaction to commit log for next processing, otherwise FALSE;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package with collection of producer and consumer transactions has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    *
    */
  def putTransactions(producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction],
                      consumerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction]): Future[Boolean] = {
    val transactions =
      (producerTransactions map (txn => Transaction(Some(txn), None))) ++
        (consumerTransactions map (txn => Transaction(None, Some(txn))))

    if (transactions.isEmpty) {
      Future.successful(true)
    } else {
      //      println("***[client] Put transactions " ++ transactions.mkString("{\n  ", "\n  ", "\n}"))

      if (logger.isDebugEnabled)
        logger.debug(s"putTransactions method is invoked: $transactions.")

      onShutdownThrowException()

      checkpointGroupInetClient.value match {
        case None =>
          checkpointGroupInetClient.flatMap { client =>
            client.method[TransactionService.PutTransactions.Args, TransactionService.PutTransactions.Result, Boolean](
              Protocol.PutTransactions,
              TransactionService.PutTransactions.Args(transactions),
              x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
            )(contextForProducerTransactions)
          }(contextForProducerTransactions)
        case Some(okOrNot) =>
          okOrNot match {
            case Success(client) =>
              client.method[TransactionService.PutTransactions.Args, TransactionService.PutTransactions.Result, Boolean](
                Protocol.PutTransactions,
                TransactionService.PutTransactions.Args(transactions),
                x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
              )(contextForProducerTransactions)
            case Failure(throwable) =>
              throw throwable
          }
      }
    }
  }

  /** Puts producer transaction on a server.
    *
    * @param transaction a producer transaction [[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]].
    * @return Future of putProducerState operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if server put producer transaction to commit log for next processing, otherwise FALSE;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putProducerState(transaction: com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction): Future[Boolean] = {
    if (logger.isDebugEnabled)
      logger.debug(s"Putting producer transaction ${transaction.transactionID} with state ${transaction.state} to stream ${transaction.stream}, partition ${transaction.partition}")
    val producerTransactionToTransaction = Transaction(Some(transaction), None)
    onShutdownThrowException()

    commonInetClient.method[TransactionService.PutTransaction.Args, TransactionService.PutTransaction.Result, Boolean](
      Protocol.PutTransaction,
      TransactionService.PutTransaction.Args(producerTransactionToTransaction),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(contextForProducerTransactions)
  }

  /** Puts consumer transaction on a server.
    *
    * @param transaction a consumer transactions.
    * @return Future of putTransaction operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if server put consumer transaction to commit log for next processing;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putTransaction(transaction: com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction): Future[Boolean] = {
    if (logger.isDebugEnabled())
      logger.debug(s"Putting consumer transaction ${transaction.transactionID} with name ${transaction.name} to stream ${transaction.stream}, partition ${transaction.partition}")
    onShutdownThrowException()

    commonInetClient.method[TransactionService.PutTransaction.Args, TransactionService.PutTransaction.Result, Boolean](
      Protocol.PutTransaction,
      TransactionService.PutTransaction.Args(Transaction(None, Some(transaction))),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(contextForProducerTransactions)
  }

  /** Puts 'simplified' producer transaction that already has Chekpointed state with it's data
    *
    * @param streamID  an id of stream.
    * @param partition a partition of stream.
    * @param data      a producer transaction data.
    * @return Future of putSimpleTransactionAndData operation that can be completed or not. If it is completed it returns:
    *         1) Transaction ID if transaction is persisted in commit log file for next processing and it's data is persisted successfully.
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putSimpleTransactionAndData(streamID: Int, partition: Int, data: Seq[Array[Byte]]): Future[Long] = {
    if (logger.isDebugEnabled) logger.debug(s"Putting 'lightweight' producer transaction to stream $streamID, partition $partition with data: $data")
    onShutdownThrowException()

    //    println(s"***[client] Producer checkpointed transaction: $streamID, $partition")
    commonInetClient.method[TransactionService.PutSimpleTransactionAndData.Args, TransactionService.PutSimpleTransactionAndData.Result, Long](
      Protocol.PutSimpleTransactionAndData,
      TransactionService.PutSimpleTransactionAndData.Args(streamID, partition, data.map(java.nio.ByteBuffer.wrap)),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(contextForProducerTransactions)
  }

  /** Puts in fire and forget policy manner 'simplified' producer transaction that already has Chekpointed state with it's data.
    *
    * @param streamID  an id of stream.
    * @param partition a partition of stream.
    * @param data      a producer transaction data.
    */
  def putSimpleTransactionAndDataWithoutResponse(streamID: Int, partition: Int, data: Seq[Array[Byte]]): Unit = {
    if (logger.isDebugEnabled) logger.debug(s"Putting 'lightweight' producer transaction to stream $streamID, partition $partition with data: $data")
    onShutdownThrowException()

    commonInetClient.methodFireAndForget[TransactionService.PutSimpleTransactionAndData.Args](
      Protocol.PutSimpleTransactionAndData,
      TransactionService.PutSimpleTransactionAndData.Args(streamID, partition, data.map(java.nio.ByteBuffer.wrap))
    )
  }

  /** Puts producer 'opened' transaction.
    *
    * @param streamID         an id of stream.
    * @param partitionID      a partition of stream.
    * @param transactionTTLMs a lifetime of producer 'opened' transaction.
    * @return Future of openTransaction operation that can be completed or not. If it is completed it returns:
    *         1) Transaction ID if transaction is persisted in commit log file for next processing.
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def openTransaction(streamID: Int, partitionID: Int, transactionTTLMs: Long): Future[Long] = {
    if (logger.isDebugEnabled)
      logger.debug(s"Putting 'lightweight' producer transaction to stream $streamID, partition $partitionID with TTL: $transactionTTLMs")
    onShutdownThrowException()

    //    println(s"***[client] Producer opened transaction: $streamID, $partitionID, $transactionTTLMs")

    commonInetClient.method[TransactionService.OpenTransaction.Args, TransactionService.OpenTransaction.Result, Long](
      Protocol.OpenTransaction,
      TransactionService.OpenTransaction.Args(streamID, partitionID, transactionTTLMs),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(contextForProducerTransactions)
  }

  /** Retrieves a producer transaction by id
    *
    * @param streamID    an id of stream.
    * @param partition   a partition of stream.
    * @param transaction a transaction id.
    * @return Future of getTransaction operation that can be completed or not. If it is completed it returns:
    *         1) Thrift TransactionInfo [[com.bwsw.tstreamstransactionserver.rpc.TransactionInfo]] which contains about whether transaction exists and producer transaction itself on case of existence;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getTransaction(streamID: Int, partition: Int, transaction: Long): Future[TransactionInfo] = {
    if (logger.isDebugEnabled()) logger.debug(s"Retrieving a producer transaction on partition '$partition' of stream '$streamID' by id '$transaction'")
    onShutdownThrowException()

    commonInetClient.method[TransactionService.GetTransaction.Args, TransactionService.GetTransaction.Result, TransactionInfo](
      Protocol.GetTransaction,
      TransactionService.GetTransaction.Args(streamID, partition, transaction),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** Retrieves last checkpointed transaction in a specific stream on certain partition; If the result is -1 it will mean there is no checkpointed transaction at all.
    *
    * @param streamID  an id of stream.
    * @param partition a partition of stream.
    * @return Future of getLastCheckpointedTransaction operation that can be completed or not. If it is completed it returns:
    *         1) Last producer transaction ID which state is "Checkpointed" on certain stream on certain partition.
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getLastCheckpointedTransaction(streamID: Int, partition: Int): Future[Long] = {
    if (logger.isDebugEnabled()) logger.debug(s"Retrieving a last checkpointed transaction on partition '$partition' of stream '$streamID")
    onShutdownThrowException()

    commonInetClient.method[TransactionService.GetLastCheckpointedTransaction.Args, TransactionService.GetLastCheckpointedTransaction.Result, Long](
      Protocol.GetLastCheckpointedTransaction,
      TransactionService.GetLastCheckpointedTransaction.Args(streamID, partition),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** Retrieves all producer transactions in a specific range [from, to]; it's assumed that "from" and "to" are both positive.
    *
    * @param streamID  an id of stream.
    * @param partition a partition of stream.
    * @param from      an inclusive bound to start with.
    * @param to        an inclusive bound to end with.
    * @return Future of scanTransactions operation that can be completed or not. If it is completed it returns:
    *         1) Thrift struct [[com.bwsw.tstreamstransactionserver.rpc.ScanTransactionsInfo]]
    *         that contains information about last opened(LT) transaction on certain stream on certain partition and
    *         producer transactions in range:
    *         If LT < from: it result is (LT, empty_collection_of_producer_transactions);
    *         If from <= LT < to: result is (LT, collection of producer transactions in range [from, LT] until producer transaction with state "Opened" met);
    *         If LT >= to: result is (LT, collection of producer transactions in range[from, to] until producer transaction with state "Opened" met).
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  @throws[Exception]
  def scanTransactions(streamID: Int, partition: Int, from: Long, to: Long, count: Int, states: Set[TransactionStates]): Future[ScanTransactionsInfo] = {
    require(from >= 0 && to >= 0, s"Calling method scanTransactions requires that bounds: 'from' and 'to' are both positive(actually from and to are: [$from, $to])")
    if (to < from || count == 0) {
      onShutdownThrowException()
      val lastOpenedTransactionID = -1L
      Future.successful(ScanTransactionsInfo(lastOpenedTransactionID, collection.immutable.Seq()))
    }
    else {
      if (logger.isDebugEnabled()) logger.debug(s"Retrieving producer transactions on stream $streamID in range [$from, $to]")
      onShutdownThrowException()

      commonInetClient.method[TransactionService.ScanTransactions.Args, TransactionService.ScanTransactions.Result, ScanTransactionsInfo](
        Protocol.ScanTransactions,
        TransactionService.ScanTransactions.Args(streamID, partition, from, to, count, states),
        x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
      )(context)
    }
  }

  /** Putting any binary data on server to a specific stream, partition, transaction id of producer transaction.
    *
    * @param streamID    an id of stream.
    * @param partition   a partition of stream.
    * @param transaction a transaction ID.
    * @param data        a data to persist.
    * @param from        an inclusive bound to strat with.
    * @return Future of putTransactionData operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if producer transaction data persisted on server successfully, otherwise FALSE.
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putTransactionData(streamID: Int, partition: Int, transaction: Long, data: Seq[Array[Byte]], from: Int): Future[Boolean] = {
    if (logger.isDebugEnabled)
      logger.debug(s"Putting transaction data to stream $streamID, partition $partition, transaction $transaction.")
    onShutdownThrowException()

    commonInetClient.method[TransactionService.PutTransactionData.Args, TransactionService.PutTransactionData.Result, Boolean](
      Protocol.PutTransactionData,
      TransactionService.PutTransactionData.Args(streamID, partition, transaction, data.map(java.nio.ByteBuffer.wrap), from),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** Putting any binary data and persisting/updating transaction state on server.
    *
    * @param producerTransaction a producer transaction contains all necessary information for persisting data.
    * @param data                a data to persist.
    * @return Future of putProducerStateWithData operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if server put producer transaction to commit log for next processing and producer transaction data persisted on server successfully,
    *         otherwise FALSE(operation isn't atomic, it's splitted to 2 operations: first putting producer state, then its data);
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putProducerStateWithData(producerTransaction: com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction, data: Seq[Array[Byte]], from: Int): Future[Boolean] = {
    import producerTransaction._
    if (logger.isDebugEnabled)
      logger.debug(
        s"Putting producer transaction to stream " +
          s"$stream, partition $partition, transaction $transactionID, state $state, ttl: $ttl, quantity: $quantity " +
          s"with data $data"
      )

    onShutdownThrowException()

    commonInetClient.method[TransactionService.PutProducerStateWithData.Args, TransactionService.PutProducerStateWithData.Result, Boolean](
      Protocol.PutProducerStateWithData,
      TransactionService.PutProducerStateWithData.Args(producerTransaction, data.map(java.nio.ByteBuffer.wrap), from),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(contextForProducerTransactions)
  }

  /** Retrieves all producer transactions binary data in a specific range [from, to]; it's assumed that "from" and "to" are both positive.
    *
    * @param streamID    an id of stream
    * @param partition   a partition of stream.
    * @param transaction a transaction id.
    * @param from        an inclusive bound to strat with.
    * @param to          an inclusive bound to end with.
    * @return Future of getTransactionData operation that can be completed or not. If it is completed it returns:
    *         1) Collection of binary data in range [from, to] on certain stream on certain partition if data presents.
    *         otherwise FALSE(operation isn't atomic, it's split into 2 operations: first putting producer state, then its data);
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist]], if there is no such stream;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         6) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         7) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getTransactionData(streamID: Int, partition: Int, transaction: Long, from: Int, to: Int): Future[Seq[Array[Byte]]] = {
    require(from >= 0 && to >= 0, s"Calling method getTransactionData requires that bounds: 'from' and 'to' are both positive(actually from and to are: [$from, $to])")
    if (to < from) {
      onShutdownThrowException()
      Future.successful(Seq[Array[Byte]]())
    }
    else {
      if (logger.isDebugEnabled) logger.debug(s"Retrieving producer transaction data from stream $streamID, partition $partition, transaction $transaction in range [$from, $to].")
      onShutdownThrowException()

      commonInetClient.method[TransactionService.GetTransactionData.Args, TransactionService.GetTransactionData.Result, Seq[Array[Byte]]](
        Protocol.GetTransactionData,
        TransactionService.GetTransactionData.Args(streamID, partition, transaction, from, to),
        x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else Implicits.byteBuffersToSeqArrayByte(x.success.get)
      )(context)
    }
  }

  /** Puts/Updates a consumer state on a specific stream, partition, transaction id on a server.
    *
    * @param consumerTransaction a consumer transaction contains all necessary information for putting/updating it's state.
    * @return Future of putConsumerCheckpoint operation that can be completed or not. If it is completed it returns:
    *         1) TRUE if server put consumer transaction to commit log for next processing, otherwise FALSE;
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def putConsumerCheckpoint(consumerTransaction: com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction): Future[Boolean] = {
    onShutdownThrowException()
    if (logger.isDebugEnabled())
      logger.debug(s"Setting consumer state ${consumerTransaction.name} on stream ${consumerTransaction.stream}, partition ${consumerTransaction.partition}, transaction ${consumerTransaction.transactionID}.")

    commonInetClient.method[TransactionService.PutConsumerCheckpoint.Args, TransactionService.PutConsumerCheckpoint.Result, Boolean](
      Protocol.PutConsumerCheckpoint,
      TransactionService.PutConsumerCheckpoint.Args(consumerTransaction.name, consumerTransaction.stream, consumerTransaction.partition, consumerTransaction.transactionID),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(contextForProducerTransactions)
  }

  /** Retrieves a consumer state on a specific consumer transaction name, stream, partition from a server; If the result is -1 it will mean there is no checkpoint at all.
    *
    * @param name      a consumer transaction name.
    * @param streamID  an id of stream.
    * @param partition a partition of the stream.
    * @return Future of getConsumerState operation that can be completed or not. If it is completed it returns:
    *         1) transaction ID on certain stream on certatin partition if it exists, otherwise -1L
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.TokenInvalidException]], if token key isn't valid;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException]], if, i.e. a request package has size in bytes more than defined by a server;
    *         4) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         5) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         6) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  def getConsumerState(name: String, streamID: Int, partition: Int): Future[Long] = {
    if (logger.isDebugEnabled) logger.debug(s"Retrieving a transaction by consumer $name on stream $streamID, partition $partition.")
    onShutdownThrowException()

    commonInetClient.method[TransactionService.GetConsumerState.Args, TransactionService.GetConsumerState.Result, Long](
      Protocol.GetConsumerState,
      TransactionService.GetConsumerState.Args(name, streamID, partition),
      x => if (x.error.isDefined) throw Throwable.byText(x.error.get.message) else x.success.get
    )(context)
  }

  /** It Disconnects client from server slightly */
  def shutdown(): Unit =
    this.synchronized {
      if (!isShutdown.get()) {
        if (workerGroup != null) {
          scala.util.Try(
            workerGroup.shutdownGracefully(
              0L,
              0L,
              TimeUnit.NANOSECONDS
            ).cancel(true))
        }
        if (commonInetClient != null)
          commonInetClient.shutdown()

        if (checkpointGroupInetClient != null)
          checkpointGroupInetClient.foreach(_.shutdown())(context)

        if (isZKClientExternal && zkConnection != null)
          zkConnection.close()

        processTransactionsPutOperationPool
          .stopAccessNewTasks()
        processTransactionsPutOperationPool
          .awaitAllCurrentTasksAreCompleted()

        executionContext
          .stopAccessNewTasksAndAwaitCurrentTasksToBeCompleted()

        isShutdown.set(true)
      }
    }

  override protected def onZKConnectionStateChanged(newState: ConnectionState): Unit =
    onZKConnectionStateChangedFunc(newState)

  override protected def onServerConnectionLost(): Unit =
    onServerConnectionLostFunc

  override protected def onRequestTimeout(): Unit =
    onRequestTimeoutFunc
}
