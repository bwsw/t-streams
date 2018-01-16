/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.exception.Throwable._
import com.bwsw.tstreamstransactionserver.netty.client.zk.{ZKConnectionLostListener, ZKMasterPathMonitor}
import com.bwsw.tstreamstransactionserver.netty.{Protocol, RequestMessage, ResponseMessage, SocketHostPortPair}
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.{TracingOptions, ZookeeperOptions}
import com.bwsw.tstreamstransactionserver.rpc.{TransactionService, TransportOptionsInfo}
import com.bwsw.tstreamstransactionserver.tracing.ClientTracer
import com.twitter.scrooge.ThriftStruct
import io.netty.buffer.ByteBuf
import io.netty.channel.EventLoopGroup
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.bytes.ByteArrayEncoder
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutorService, Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

class InetClient(zookeeperOptions: ZookeeperOptions,
                 connectionOptions: ConnectionOptions,
                 authOpts: AuthOptions,
                 onServerConnectionLost: => Unit,
                 onRequestTimeout: => Unit,
                 onZKConnectionStateChanged: ConnectionState => Unit,
                 workerGroup: EventLoopGroup,
                 isShutdown: => Boolean,
                 zkConnection: CuratorFramework,
                 requestIDGen: AtomicLong,
                 requestIdToResponseMap: ConcurrentHashMap[Long, Promise[ByteBuf]],
                 context: ExecutionContextExecutorService,
                 tracingOptions: TracingOptions = TracingOptions()) {

  private val tracer = ClientTracer(tracingOptions)

  private val logger =
    LoggerFactory.getLogger(this.getClass)

  private val isAuthenticating =
    new java.util.concurrent.atomic.AtomicBoolean(false)

  private val zkConnectionLostListener = new ZKConnectionLostListener(
    zkConnection,
    onZKConnectionStateChanged
  )

  private val commonServerPathMonitor =
    new ZKMasterPathMonitor(
      zkConnection,
      connectionOptions.retryDelayMs,
      connectionOptions.prefix
    )

  commonServerPathMonitor
    .startMonitoringMasterServerPath()

  private val connected = new AtomicBoolean(false)
  private val isStopped = new AtomicBoolean(false)
  private val maybeFailCause = new AtomicReference[Option[Throwable]](None)

  private val nettyClient: NettyConnection = {
    val connectionAddress =
      commonServerPathMonitor.getMasterInBlockingManner

    connectionAddress match {
      case Left(throwable) =>
        shutdown()
        throw throwable
      case Right(socket) =>
        val client = new NettyConnection(
          workerGroup,
          socket,
          connectionOptions,
          handlers, {
            onServerConnectionLost
            Future {
              onServerConnectionLostDefaultBehaviour()
            }(context)
          }
        )

        connected.set(true)
        commonServerPathMonitor.addMasterReelectionListener { newMaster =>
          shutdown()
          val exception = newMaster match {
            case Left(throwable) => throwable
            case Right(None) => new MasterLostException
            case Right(Some(socketHostPortPair)) => new MasterChangedException(socketHostPortPair)
          }
          maybeFailCause.compareAndSet(None, Some(exception))
          throw exception
        }

        client
    }
  }

  @volatile private var currentToken: Int = -1
  @volatile private var messageSizeValidator: MessageSizeValidator =
    new MessageSizeValidator(Int.MaxValue, Int.MaxValue)

  /** A general method for sending requests to a server and getting a response back.
    *
    * @param descriptor look at [[com.bwsw.tstreamstransactionserver.netty.Protocol]].
    * @param request    a request that client would like to send.
    * @return a response from server(however, it may return an exception from server).
    *
    */
  final def method[Req <: ThriftStruct, Rep <: ThriftStruct, A](descriptor: Protocol.Descriptor[Req, Rep],
                                                                request: Req,
                                                                f: Rep => A
                                                               )(implicit methodContext: concurrent.ExecutionContext): Future[A] = {
    methodWithMessageSizeValidation(
      descriptor,
      request,
      f
    )
  }

  @throws[TokenInvalidException]
  @throws[PackageTooBigException]
  final def methodFireAndForget[Req <: ThriftStruct](descriptor: Protocol.Descriptor[Req, _],
                                                     request: Req): Unit = {
    onNotConnectedThrowException()

    if (getToken == -1)
      authenticate()

    val messageId = requestIDGen.getAndIncrement()
    val message = descriptor.encodeRequestToMessage(request)(messageId, getToken, isFireAndForgetMethod = true)

    if (logger.isDebugEnabled) logger.debug(Protocol.methodWithArgsToString(messageId, request))
    messageSizeValidator.validateMessageSize(message)

    val channel = nettyClient.getChannel()
    val binaryMessage =
      message.toByteBuf(channel.alloc())
    channel.eventLoop().execute(() =>
      channel.writeAndFlush(binaryMessage, channel.voidPromise())
    )
  }

  final def currentConnectionSocketAddress: Either[Throwable, Option[SocketHostPortPair]] =
    commonServerPathMonitor.getCurrentMaster

  final def getZKCheckpointGroupServerPrefix(): Future[String] = {
    if (logger.isInfoEnabled)
      logger.info("getMaxPackagesSizes method is invoked.")

    onShutdownThrowException()

    methodWithoutMessageSizeValidation[TransactionService.GetZKCheckpointGroupServerPrefix.Args, TransactionService.GetZKCheckpointGroupServerPrefix.Result, String](
      Protocol.GetZKCheckpointGroupServerPrefix,
      TransactionService.GetZKCheckpointGroupServerPrefix.Args(),
      x => x.success.get
    )(context)
  }

  private final def onServerConnectionLostDefaultBehaviour(): Unit = {
    val exception = maybeFailCause.get().getOrElse {
      val connectionSocket = commonServerPathMonitor.getCurrentMaster
        .toOption
        .flatten
        .map(_.toString)
        .getOrElse("NO_CONNECTION_SOCKET")

      new ServerUnreachableException(connectionSocket)
    }

    requestIdToResponseMap.elements().asScala
      .foreach(_.tryFailure(exception))
  }

  private def handlers = {
    Seq(
      new ByteArrayEncoder(),
      new LengthFieldBasedFrameDecoder(
        Int.MaxValue,
        ResponseMessage.headerFieldSize,
        ResponseMessage.lengthFieldSize
      ),
      new ClientHandler(requestIdToResponseMap)
    )
  }

  private def onShutdownThrowException(): Unit =
    if (isShutdown) throw new ClientIllegalOperationAfterShutdown

  private def onNotConnectedThrowException(): Unit = {
    if (!isConnected) {
      throw maybeFailCause.get().getOrElse(new ClientNotConnectedException)
    }
  }


  private def sendRequest[Req <: ThriftStruct, Rep <: ThriftStruct, A](message: RequestMessage,
                                                                       descriptor: Protocol.Descriptor[Req, Rep],
                                                                       f: Rep => A,
                                                                       previousException: Option[Throwable] = None,
                                                                       retryCount: Int = Int.MaxValue)
                                                                      (implicit methodContext: concurrent.ExecutionContext): Future[A] = {
    onNotConnectedThrowException()
    val updatedMessage = tracer.clientSend(message)
    val promise = Promise[ByteBuf]
    requestIdToResponseMap.put(updatedMessage.id, promise)

    val channel = nettyClient.getChannel()
    val binaryResponse =
      updatedMessage.toByteBuf(channel.alloc())

    val responseFuture = TimeoutScheduler.withTimeout(
      (updatedMessage.tracingInfo match {
        case Some(tracingInfo) => promise.future.transform { tried =>
          tracer.clientReceive(tracingInfo)
          tried
        }
        case None => promise.future
      }).map { response =>
        requestIdToResponseMap.remove(updatedMessage.id)
        f(descriptor.decodeResponse(response))
      }
    )(connectionOptions.requestTimeoutMs.millis,
      updatedMessage.id
    )(methodContext)

    channel.eventLoop().execute(() =>
      channel.writeAndFlush(binaryResponse, channel.voidPromise())
    )

    responseFuture.recoverWith { case error =>
      requestIdToResponseMap.remove(updatedMessage.id)
      val (currentException, counter) =
        checkError(error, previousException, retryCount)
      if (counter == 0) {
        Future.failed(currentException)
      }
      else {
        val messageId = requestIDGen.getAndIncrement()
        val newMessage = updatedMessage.copy(
          id = messageId,
          token = getToken
        )
        sendRequest(newMessage, descriptor, f, Some(currentException), counter)
      }
    }(methodContext)
  }

  private def methodWithMessageSizeValidation[Req <: ThriftStruct, Rep <: ThriftStruct, A](descriptor: Protocol.Descriptor[Req, Rep],
                                                                                           request: Req,
                                                                                           f: Rep => A)
                                                                                          (implicit methodContext: concurrent.ExecutionContext): Future[A] = {
    val messageId = requestIDGen.getAndIncrement()

    val message = descriptor.encodeRequestToMessage(request)(
      messageId,
      getToken,
      isFireAndForgetMethod = false
    )

    if (getToken == -1)
      authenticate()

    messageSizeValidator.validateMessageSize(message)

    sendRequest(message, descriptor, f)
  }

  private def methodWithoutMessageSizeValidation[Req <: ThriftStruct, Rep <: ThriftStruct, A](descriptor: Protocol.Descriptor[Req, Rep],
                                                                                              request: Req,
                                                                                              f: Rep => A)
                                                                                             (implicit methodContext: concurrent.ExecutionContext): Future[A] = {
    val messageId = requestIDGen.getAndIncrement()

    val message = descriptor.encodeRequestToMessage(request)(
      messageId,
      getToken,
      isFireAndForgetMethod = false
    )

    sendRequest(message, descriptor, f)
  }

  private final def checkError(currentException: Throwable,
                               previousException: Option[Throwable],
                               retryCount: Int): (Throwable, Int) = {
    currentException match {
      case tokenException: TokenInvalidException =>
        authenticate()
        previousException match {
          case Some(_: TokenInvalidException) =>
            (tokenException, retryCount - 1)
          case _ =>
            (tokenException, connectionOptions.requestTimeoutRetryCount)
        }

      case concreteThrowable: ServerUnreachableException =>
        Try(onServerConnectionLostDefaultBehaviour()) match {
          case Failure(throwable) => (throwable, 0)
          case Success(_) => (concreteThrowable, 0)
        }

      case concreteThrowable: RequestTimeoutException =>
        maybeFailCause.compareAndSet(None, Some(concreteThrowable))
        Try(onRequestTimeout) match {
          case Failure(throwable) => (throwable, 0)
          case Success(_) => (concreteThrowable, 0)
        }

      case otherThrowable =>
        (otherThrowable, 0)
    }
  }

  private final def getToken: Int = {
    currentToken
  }

  /** Retrieves a token for that allow a client send requests to server.
    *
    * @return Future of authenticate operation that can be completed or not. If it is completed it returns:
    *         1) token - for authorizing,
    *         maxDataPackageSize - max size of package into putTransactionData method with its arguments wrapped,
    *         maxMetadataPackageSize - max size of package into all kind of that operations with producer and consumer transactions methods with its arguments wrapped,
    *         2) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException]], if, i.e. client had sent this request to a server, but suddenly server would have been shutdowned,
    *         and, as a result, request din't reach the server, and client tried to get the new server from zooKeeper but there wasn't one on coordination path;
    *         3) throwable [[com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown]] if client try to call this function after shutdown.
    *         4) other kind of exceptions that mean there is a bug on a server, and it is should to be reported about this issue.
    */
  private def authenticate(): Unit = {
    if (logger.isInfoEnabled)
      logger.info("authenticate method is invoked.")

    onShutdownThrowException()

    val needToAuthenticate =
      isAuthenticating.compareAndSet(false, true)

    if (needToAuthenticate) {
      val authKey = authOpts.key
      val latch = new CountDownLatch(1)
      methodWithoutMessageSizeValidation[TransactionService.Authenticate.Args, TransactionService.Authenticate.Result, Unit](
        Protocol.Authenticate,
        TransactionService.Authenticate.Args(authKey),
        x => {
          val tokenFromServer = x.success.get
          currentToken = tokenFromServer

          val packageSizes = getMaxPackagesSizes()
          messageSizeValidator = new MessageSizeValidator(
            packageSizes.maxMetadataPackageSize,
            packageSizes.maxDataPackageSize
          )

          latch.countDown()
        }
      )(context)

      latch.await(
        connectionOptions.requestTimeoutMs,
        TimeUnit.MILLISECONDS
      )

      isAuthenticating.set(false)
    } else {
      while (isAuthenticating.get()) {}
    }
  }

  private def getMaxPackagesSizes(): TransportOptionsInfo = {
    if (logger.isInfoEnabled)
      logger.info("getMaxPackagesSizes method is invoked.")

    onShutdownThrowException()

    val latch = new CountDownLatch(1)
    var transportOptionsInfo: TransportOptionsInfo = null
    methodWithoutMessageSizeValidation[TransactionService.GetMaxPackagesSizes.Args, TransactionService.GetMaxPackagesSizes.Result, Unit](
      Protocol.GetMaxPackagesSizes,
      TransactionService.GetMaxPackagesSizes.Args(),
      x => {
        transportOptionsInfo = x.success.get
        latch.countDown()
      }
    )(context)

    latch.await(
      connectionOptions.requestTimeoutMs,
      TimeUnit.MILLISECONDS
    )
    transportOptionsInfo
  }

  def shutdown(): Unit = {
    if (!isStopped.getAndSet(true)) {
      commonServerPathMonitor.stopMonitoringMasterServerPath()
      if (connected.getAndSet(false))
        nettyClient.stop()
      zkConnectionLostListener.stop()
      tracer.close()
    }
  }

  def isConnected: Boolean = connected.get()
}
