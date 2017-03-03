package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import com.bwsw.tstreamstransactionserver.`implicit`.Implicits._
import com.bwsw.tstreamstransactionserver.configProperties.ClientExecutionContext
import com.bwsw.tstreamstransactionserver.exception.Throwable
import com.bwsw.tstreamstransactionserver.exception.Throwable.{RequestTimeoutException, _}
import com.bwsw.tstreamstransactionserver.netty.{Descriptors, ExecutionContext}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.zooKeeper.ZKLeaderClientToGetMaster
import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.twitter.scrooge.ThriftStruct
import io.netty.bootstrap.Bootstrap
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener, ChannelOption}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.curator.retry.RetryForever
import org.slf4j.LoggerFactory
import transactionService.rpc.{TransactionService, _}


import scala.annotation.tailrec
import scala.concurrent.{Future => ScalaFuture, Promise => ScalaPromise}


/** A client who connects to a server.
  *
  * @constructor create a new client by configuration file or map.
  */
class Client(clientOpts: ConnectionOptions, authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private final val executionContext = new ClientExecutionContext(clientOpts.threadPool)

  private final val zkListener = new ConnectionStateListener {
    override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
      onZKConnectionStateChangedDefaultBehaviour(newState)
    }
  }

  private final val zKLeaderClient = new ZKLeaderClientToGetMaster(zookeeperOpts.endpoints,
    zookeeperOpts.sessionTimeoutMs, zookeeperOpts.connectionTimeoutMs,
    new RetryForever(zookeeperOpts.retryDelayMs), zookeeperOpts.prefix, zkListener)
  zKLeaderClient.start()

  private final def onZKConnectionStateChangedDefaultBehaviour(newState: ConnectionState): Unit = {
    newState match {
      case ConnectionState.LOST => zKLeaderClient.master = None
      case _ => ()
    }
    onZKConnectionStateChanged(newState)
  }

  private implicit final val context = executionContext.context

  private final val nextSeqId = new AtomicInteger(1)

  private final val expiredRequestInvalidator = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("ExpiredRequestInvalidator-%d").build())
  private final val reqIdToRep: Cache[Integer, ScalaPromise[ThriftStruct]] = CacheBuilder.newBuilder()
    .concurrencyLevel(clientOpts.threadPool)
    .expireAfterWrite(clientOpts.requestTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
    .removalListener(new RemovalListener[java.lang.Integer, ScalaPromise[ThriftStruct]] {
        override def onRemoval(notification: RemovalNotification[java.lang.Integer, ScalaPromise[ThriftStruct]]): Unit = {
          notification.getValue.tryFailure(new RequestTimeoutException(notification.getKey, clientOpts.requestTimeoutMs))
        }
      }
    ).build[java.lang.Integer, ScalaPromise[ThriftStruct]]()

  expiredRequestInvalidator.scheduleAtFixedRate(() => reqIdToRep.cleanUp(), 0, clientOpts.connectionTimeoutMs, TimeUnit.MILLISECONDS)

  private val workerGroup = new EpollEventLoopGroup()

  private val bootstrap = new Bootstrap()
    .group(workerGroup)
    .channel(classOf[EpollSocketChannel])
    .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
    .handler(new ClientInitializer(reqIdToRep, this, context))


  @volatile private var channel: Channel = _
  connect()

  def connect(): Unit = {
    val (listen, port) = getInetAddressFromZookeeper(clientOpts.requestTimeoutRetryCount)
    bootstrap.connect(listen, port).addListener(new ConnectionListener)
  }

  private class ConnectionListener extends ChannelFutureListener() {
    val atomicInteger = new AtomicInteger(clientOpts.connectionTimeoutMs / clientOpts.retryDelayMs)

    @throws[ServerConnectionException]
    override def operationComplete(channelFuture: ChannelFuture): Unit = {
      if (!channelFuture.isSuccess && atomicInteger.getAndDecrement() > 0) {
        //      System.out.println("Reconnect")
        val loop = channelFuture.channel().eventLoop()
        loop.execute(() => {
          TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
          connect()
        })
      } else if (atomicInteger.get() <= 0) throw new ServerConnectionException
      else channel = channelFuture.sync().channel()
    }
  }


  /** Retries to get ipAddres:port from zooKeeper server.
    *
    * @param times how many times try to get ipAddres:port from zooKeeper server.
    */
  @tailrec @throws[ZkGetMasterException]
  private def getInetAddressFromZookeeper(times: Int): (String, Int) = {
    if (times > 0 && zKLeaderClient.master.isEmpty) {
      TimeUnit.MILLISECONDS.sleep(zookeeperOpts.retryDelayMs)
      if (logger.isInfoEnabled) logger.info("Retrying to get master server from zookeeper server.")
      getInetAddressFromZookeeper(times - 1)
    } else {
      zKLeaderClient.master match {
        case Some(master) => val listenPort = master.split(":")
          (listenPort(0), listenPort(1).toInt)
        case None => {
          if (logger.isErrorEnabled) logger.error(Throwable.zkGetMasterExceptionMessage)
          shutdown()
          throw new ZkGetMasterException
        }
      }
    }
  }

  private val barrier = new ResettableCountDownLatch(1)
  @volatile private var isChannelCanBeUsed = true
  private def applyBarrierIfItIsRequired():Unit = {
    if (!isChannelCanBeUsed) barrier.countDown()
  }
  private def resetBarrier(f: => Unit) = {
    isChannelCanBeUsed = false
    f
    isChannelCanBeUsed = true
    barrier.reset
  }
  private def resetBarrier(f: => ScalaFuture[Unit]) = {
    isChannelCanBeUsed = false
    f map  { _ =>
      isChannelCanBeUsed = true
      barrier.reset
    }
  }


  /** A general method for sending requests to a server and getting a response back.
    *
    * @param descriptor look at [[com.bwsw.tstreamstransactionserver.netty.Descriptors]].
    * @param request a request that client would like to send.
    *
    * @return a response from server(however, it may return an exception from server).
    *
    */
  @throws[ServerUnreachableException]
  private def method[Req <: ThriftStruct, Rep <: ThriftStruct](descriptor: Descriptors.Descriptor[Req, Rep], request: Req)
                                                              (implicit context: concurrent.ExecutionContext): ScalaFuture[Rep] = {
    applyBarrierIfItIsRequired()
    if (channel != null && channel.isActive) {
      val messageId = nextSeqId.getAndIncrement()
      val promise = ScalaPromise[ThriftStruct]
      val message = descriptor.encodeRequest(request)(messageId)
      reqIdToRep.put(messageId, promise)
      channel.writeAndFlush(message.toByteArray)
      promise.future.map { response =>
        reqIdToRep.invalidate(messageId)
        response.asInstanceOf[Rep]
      }.recoverWith { case error =>
        reqIdToRep.invalidate(messageId)
        ScalaFuture.failed(error)
      }
    } else ScalaFuture.failed(new ServerUnreachableException)
  }


  private def retry[Req, Rep](f: => ScalaFuture[Rep])(previousException: Throwable, retryCount: Int): ScalaFuture[Rep] = {
    def helper(throwable: Throwable, retryCount: Int): ScalaFuture[Rep] = {
      if (retryCount > 0) {
        if (throwable.getClass equals previousException.getClass)
          retry(f)(throwable, retryCount)
        else
          tryCompleteRequest(f)
      } else {
        if (throwable.getClass equals classOf[RequestTimeoutException]) {

          resetBarrier {
            channel.close()
            connect()
          }

          tryCompleteRequest(f)
        } else ScalaFuture.failed(throwable)
      }
    }

    f recoverWith {
      case tokenInvalidThrowable: TokenInvalidException =>
        if (logger.isWarnEnabled)
          logger.warn("Token isn't valid. Retrying get one.")

        resetBarrier(authenticate()) flatMap { _ =>
          TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
          helper(tokenInvalidThrowable, retryCount - 1)
        }

      case serverUnreachableThrowable: ServerUnreachableException =>
        scala.util.Try(onServerConnectionLostDefaultBehaviour()) match {
          case scala.util.Success(_) => helper(serverUnreachableThrowable, retryCount)
          case scala.util.Failure(throwable) => ScalaFuture.failed(throwable)
        }
      case requestTimeout: RequestTimeoutException =>
        scala.util.Try(onRequestTimeout()) match {
          case scala.util.Success(_) => helper(requestTimeout, retryCount - 1)
          case scala.util.Failure(throwable) => ScalaFuture.failed(throwable)
        }
      case otherThrowable =>
        ScalaFuture.failed(otherThrowable)
    }
  }

  private def tryCompleteRequest[Req, Rep](f: => ScalaFuture[Rep]) = {
    f recoverWith {
      case concreteThrowable: TokenInvalidException =>
        if (logger.isWarnEnabled) logger.warn("Token isn't valid. Retrying get one.")
        authenticate()
        TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
        retry(f)(concreteThrowable, clientOpts.requestTimeoutRetryCount)

      case concreteThrowable: ServerUnreachableException =>
        scala.util.Try(onServerConnectionLostDefaultBehaviour()) match {
          case scala.util.Success(_) => retry(f)(concreteThrowable, Int.MaxValue)
          case scala.util.Failure(throwable) => ScalaFuture.failed(throwable)
        }
      case concreteThrowable: RequestTimeoutException =>
        scala.util.Try(onRequestTimeout()) match {
          case scala.util.Success(_) =>  retry(f)(concreteThrowable, clientOpts.requestTimeoutRetryCount)
          case scala.util.Failure(throwable) => ScalaFuture.failed(throwable)
        }
      case otherThrowable =>
        ScalaFuture.failed(otherThrowable)
    }
  }

  private def onServerConnectionLostDefaultBehaviour(): Unit = {
    if (logger.isWarnEnabled)
      logger.warn(s"${Throwable.serverUnreachableExceptionMessage}. Retrying to reconnect server.")
    TimeUnit.MILLISECONDS.sleep(clientOpts.retryDelayMs)
    onServerConnectionLost()
  }

  protected def onZKConnectionStateChanged(newState: ConnectionState): Unit = {}
  protected def onServerConnectionLost(): Unit = {}
  protected def onRequestTimeout(): Unit = {}


  @volatile private var token: Int = _

  /** Putting a stream on a server by primitive type parameters.
    *
    * @param stream a name of stream.
    * @param partitions a number of stream partitions.
    * @param description a description of stream.
    *
    * @return placeholder of putStream operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info(s"Putting stream $stream with $partitions partitions, ttl $ttl and description.")
    tryCompleteRequest(
      method(
        Descriptors.PutStream,
        TransactionService.PutStream.Args(token, stream, partitions, description, ttl)
      ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  /**  Putting a stream on a server by Thrift Stream structure.
    *
    * @param stream an object of Stream structure.
    *
    * @return placeholder of putStream operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def putStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info(s"Putting stream ${stream.name} with ${stream.partitions} partitions, ttl ${stream.ttl} and description.")
    tryCompleteRequest(
      method(
        Descriptors.PutStream,
        TransactionService.PutStream.Args(token, stream.name, stream.partitions, stream.description, stream.ttl)
      ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  /** Deleting a stream by name on a server.
    *
    * @param stream a name of stream.
    *
    * @return placeholder of delStream operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def delStream(stream: String): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info(s"Deleting stream $stream.")
    tryCompleteRequest(
      method(
        Descriptors.DelStream,
        TransactionService.DelStream.Args(token, stream)
      ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  /** Deleting a stream by Thrift Stream structure on a server.
    *
    * @param stream a name of stream.
    *
    * @return placeholder of delStream operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def delStream(stream: transactionService.rpc.Stream): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info(s"Deleting stream ${stream.name}.")
    tryCompleteRequest(
      method(
        Descriptors.DelStream,
        TransactionService.DelStream.Args(token, stream.name)
      ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  /** Retrieving a stream from a server by it's name.
    *
    * @param stream a name of stream.
    *
    * @return placeholder of getStream operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def getStream(stream: String): ScalaFuture[transactionService.rpc.Stream] = {
    if (logger.isInfoEnabled) logger.info(s"Retrieving stream $stream.")
    tryCompleteRequest(
      method(
        Descriptors.GetStream,
        TransactionService.GetStream.Args(token, stream)
      ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }


  /** Checks by a stream's name that stream saved in database on server.
    *
    * @param stream a name of stream.
    *
    * @return placeholder of doesStream operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def checkStreamExists(stream: String): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info(s"Checking stream $stream on existence...")
    tryCompleteRequest(
      method(
        Descriptors.CheckStreamExists,
        TransactionService.CheckStreamExists.Args(token, stream)
      ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  /** A special context for making requests asynchronously, although they are processed sequentially;
    * If's for: putTransaction, putTransactions, setConsumerState.
    */
  private final val futurePool = ExecutionContext(1, "ClientTransactionPool-%d")


  /** Puts producer and consumer transactions on a server; it's implied there were persisted streams on a server transactions belong to, otherwise
    * the exception would be thrown.
    *
    * @param producerTransactions some collections of producer transactions.
    * @param consumerTransactions some collections of consumer transactions.
    *
    * @return placeholder of putTransactions operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def putTransactions(producerTransactions: Seq[transactionService.rpc.ProducerTransaction],
                      consumerTransactions: Seq[transactionService.rpc.ConsumerTransaction]): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled) logger.info("putTransactions method is invoked.")

    val transactions =
      (producerTransactions map (txn => Transaction(Some(txn), None))) ++
        (consumerTransactions map (txn => Transaction(None, Some(txn))))

    implicit val context = futurePool.getContext
    tryCompleteRequest(
      method(
        Descriptors.PutTransactions,
        TransactionService.PutTransactions.Args(token, transactions)
      ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  /** Puts producer transaction on a server; it's implied there was persisted stream on a server transaction belong to, otherwise
    * the exception would be thrown.
    *
    * @param transaction a producer transactions.
    *
    * @return placeholder of putTransaction operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def putTransaction(transaction: transactionService.rpc.ProducerTransaction): ScalaFuture[Boolean] = {
    implicit val context = futurePool.getContext
    if (logger.isInfoEnabled) logger.info(s"Putting producer transaction ${transaction.transactionID} with state ${transaction.state} to stream ${transaction.stream}, partition ${transaction.partition}")
    TransactionService.PutTransaction.Args(token, Transaction(Some(transaction), None))
    tryCompleteRequest(
      method(
        Descriptors.PutTransaction,
        TransactionService.PutTransaction.Args(token, Transaction(Some(transaction), None))
      ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }


  /** Puts consumer transaction on a server; it's implied there was persisted stream on a server transaction belong to, otherwise
    * the exception would be thrown.
    *
    * @param transaction a consumer transactions.
    *
    * @return placeholder of putTransaction operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def putTransaction(transaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    implicit val context = futurePool.getContext
    if (logger.isInfoEnabled) logger.info(s"Putting consumer transaction ${transaction.transactionID} with name ${transaction.name} to stream ${transaction.stream}, partition ${transaction.partition}")
    tryCompleteRequest(
      method(
        Descriptors.PutTransaction,
        TransactionService.PutTransaction.Args(token, Transaction(None, Some(transaction)))
      ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  /** Retrieves all producer transactions in a specific range [from; to); it's assumed that from >= to and they are both positive.
    *
    *
    * @param stream a name of stream.
    * @param partition a partition of stream.
    * @param from an inclusive bound to strat with.
    * @param to an exclusive bound to end with.
    *
    * @return placeholder of putTransaction operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def scanTransactions(stream: String, partition: Int, from: Long, to: Long): ScalaFuture[Seq[transactionService.rpc.ProducerTransaction]] = {
    require(from >= 0 && to >= 0)
    if (to < from)
      ScalaFuture.successful(Seq[transactionService.rpc.ProducerTransaction]())
    else {
      if (logger.isInfoEnabled) logger.info(s"Retrieving producer transactions on stream $stream in range [$from, $to]")
      tryCompleteRequest(
        method(
          Descriptors.ScanTransactions,
          TransactionService.ScanTransactions.Args(token, stream, partition, from, to)
        ).flatMap(x =>
          if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message))
          else ScalaFuture.successful(x.success.get.withFilter(_.consumerTransaction.isEmpty).map(_.producerTransaction.get))
        )
      )
    }
  }

  /** Putting any binary data on server to a specific stream, partition, transaction id of producer tranasaction.
    *
    *
    * @param stream a stream.
    * @param partition a partition of stream.
    * @param transaction a transaction ID.
    * @param data a data to persist.
    * @param from an inclusive bound to strat with.
    *
    * @return placeholder of putTransaction operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def putTransactionData(stream: String, partition: Int, transaction: Long, data: Seq[Array[Byte]], from: Int) = {
    if (logger.isInfoEnabled) logger.info(s"Putting transaction data to stream $stream, partition $partition, transaction $transaction.")
    tryCompleteRequest(
      method(
        Descriptors.PutTransactionData,
        TransactionService.PutTransactionData.Args(token, stream, partition, transaction, data, from)
      ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  /** Putting any binary data and setting transaction state on server.
    *
    *
    * @param producerTransaction a producer transaction contains all necessary information for persisting data.
    * @param data a data to persist.
    *
    * @return placeholder of putTransactionData operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def putTransactionData(producerTransaction: transactionService.rpc.ProducerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean] = {
    putTransaction(producerTransaction) flatMap {response =>
      if (logger.isInfoEnabled) logger.info(s"Putting transaction data to stream ${producerTransaction.stream}, partition ${producerTransaction.partition}, transaction ${producerTransaction.transactionID}.")
      tryCompleteRequest(
        method(
          Descriptors.PutTransactionData,
          TransactionService.PutTransactionData.Args(token, producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID, data, from)
        ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
      )
    }
  }


  /** Retrieves all producer transactions binary data in a specific range [from; to]; it's assumed that from >= to and they are both positive.
    *
    *
    * @param stream a stream
    * @param partition a partition of stream.
    * @param transaction a transaction id.
    * @param from an inclusive bound to strat with.
    * @param to an inclusive bound to end with.
    *
    * @return placeholder of getTransactionData operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def getTransactionData(stream: String, partition: Int, transaction: Long, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]] = {
    require(from >= 0 && to > 0)
    if (to < from) return ScalaFuture.successful(Seq[Array[Byte]]())
    if (logger.isInfoEnabled) logger.info(s"Retrieving producer transaction data from stream $stream, partition $partition, transaction $transaction in range [$from, $to].")
    tryCompleteRequest(
      method(
        Descriptors.GetTransactionData,
        TransactionService.GetTransactionData.Args(token, stream, partition, transaction, from, to)
      ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(byteBuffersToSeqArrayByte(x.success.get)))
    )
  }


  /** Puts/Updates a consumer state on a specific stream, partition, transaction id on a server.
    *
    *
    * @param consumerTransaction a consumer transaction contains all necessary information for putting/updating it's state.
    *
    * @return placeholder of setConsumerState operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def setConsumerState(consumerTransaction: transactionService.rpc.ConsumerTransaction): ScalaFuture[Boolean] = {
    if (logger.isInfoEnabled)
      logger.info(s"Setting consumer state ${consumerTransaction.name} on stream ${consumerTransaction.stream}, partition ${consumerTransaction.partition}, transaction ${consumerTransaction.transactionID}.")
    tryCompleteRequest(
      method(
        Descriptors.SetConsumerState,
        TransactionService.SetConsumerState.Args(token, consumerTransaction.name, consumerTransaction.stream, consumerTransaction.partition, consumerTransaction.transactionID)
      ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))(futurePool.getContext)
    )
  }

  /** Retrieves a consumer state on a specific consumer transaction name, stream, partition from a server.
    *
    * @param name a consumer transaction name.
    * @param stream a stream.
    * @param partition a partition of the stream.
    *
    * @return placeholder of getConsumerState operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  @throws[Exception]
  def getConsumerState(name: String, stream: String, partition: Int): ScalaFuture[Long] = {
    logger.info(s"Retrieving a transaction by consumer $name on stream $stream, partition $partition.")
    tryCompleteRequest(
      method(
        Descriptors.GetConsumerState,
        TransactionService.GetConsumerState.Args(token, name, stream, partition)
      ).flatMap(x => if (x.error.isDefined) ScalaFuture.failed(Throwable.byText(x.error.get.message)) else ScalaFuture.successful(x.success.get))
    )
  }

  /** Retrieves a token for that allow a client send requests to server.
    *
    * @return placeholder of authenticate operation that can be completed or not. If the method returns failed future it means
    *         a server can't handle the request and interrupt a client to do any requests by throwing an exception.
    */
  private def authenticate(): ScalaFuture[Unit] = {
    if (logger.isInfoEnabled) logger.info("authenticate method is invoked.")
    val authKey = authOpts.key
    method(
      Descriptors.Authenticate,
      TransactionService.Authenticate.Args(authKey)
    ).map(x => token = x.success.get)
  }

  def shutdown() = {
    zKLeaderClient.close()
    workerGroup.shutdownGracefully()
    if (channel != null) channel.closeFuture().sync()
    futurePool.shutdown()
    executionContext.shutdown()
  }
}