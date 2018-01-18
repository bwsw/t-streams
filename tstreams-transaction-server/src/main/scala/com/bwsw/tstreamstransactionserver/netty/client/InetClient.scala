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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import javax.naming.AuthenticationException

import com.bwsw.tstreamstransactionserver.exception.Throwable._
import com.bwsw.tstreamstransactionserver.netty.client.zk.{ZKConnectionLostListener, ZKMasterPathMonitor}
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
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

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutorService, Future, Promise}
import scala.util.{Failure, Success, Try}


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

  private val logger = LoggerFactory.getLogger(getClass)

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

  commonServerPathMonitor.startMonitoringMasterServerPath()

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
          setFailCause(exception)
          throw exception
        }

        client
    }
  }

  private val token = authenticate()
  private val transportOptionsInfo = getMaxPackagesSizes()
  private val messageSizeValidator = new MessageSizeValidator(
    transportOptionsInfo.maxMetadataPackageSize,
    transportOptionsInfo.maxDataPackageSize)

  /** A general method for sending requests to a server and getting a response back.
    *
    * @param descriptor look at [[com.bwsw.tstreamstransactionserver.netty.Protocol]].
    * @param request    a request that client would like to send.
    * @return a response from server(however, it may return an exception from server).
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

    val messageId = requestIDGen.getAndIncrement()
    val message = descriptor.encodeRequestToMessage(request)(messageId, token, isFireAndForgetMethod = true)

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

  private def onShutdownThrowException(): Unit = {
    if (isShutdown) {
      throw new ClientIllegalOperationAfterShutdownException
    }
  }

  private def onNotConnectedThrowException(): Unit = {
    if (!isConnected) {
      throw maybeFailCause.get().getOrElse(new ClientNotConnectedException)
    }
  }


  private def sendRequest[Req <: ThriftStruct, Rep <: ThriftStruct, A](message: RequestMessage,
                                                                       descriptor: Protocol.Descriptor[Req, Rep],
                                                                       f: Rep => A)
                                                                      (implicit methodContext: concurrent.ExecutionContext): Future[A] = {
    onNotConnectedThrowException()
    val updatedMessage = tracer.clientSend(message)
    val promise = Promise[ByteBuf]
    requestIdToResponseMap.put(updatedMessage.id, promise)

    val channel = nettyClient.getChannel()
    val binaryResponse = updatedMessage.toByteBuf(channel.alloc())

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

    responseFuture.recoverWith {
      case error =>
        requestIdToResponseMap.remove(updatedMessage.id)
        Try(handleException(error))
        setFailCause(error)

        Future.failed(error)
    }(methodContext)
  }

  private def methodWithMessageSizeValidation[Req <: ThriftStruct, Rep <: ThriftStruct, A](descriptor: Protocol.Descriptor[Req, Rep],
                                                                                           request: Req,
                                                                                           f: Rep => A)
                                                                                          (implicit methodContext: concurrent.ExecutionContext): Future[A] = {


    val messageId = requestIDGen.getAndIncrement()

    val message = descriptor.encodeRequestToMessage(request)(
      messageId,
      token,
      isFireAndForgetMethod = false
    )

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
      token,
      isFireAndForgetMethod = false
    )

    sendRequest(message, descriptor, f)
  }

  private final def handleException(exception: Throwable) = {
    exception match {
      case _: ServerUnreachableException =>
        onServerConnectionLostDefaultBehaviour()
        onServerConnectionLost

      case _: RequestTimeoutException =>
        onRequestTimeout

      case _ =>
    }
  }

  /** Retrieves a token for that allow a client send requests to server.
    *
    * @return retrieved token
    */
  private def authenticate(): Int = {
    if (logger.isInfoEnabled)
      logger.info("authenticate method is invoked.")

    onShutdownThrowException()

    @tailrec
    def inner(): Int = {
      if (isConnected) {
        val message = Protocol.Authenticate.encodeRequestToMessage(
          TransactionService.Authenticate.Args(authOpts.key))(
          requestIDGen.getAndIncrement(),
          AuthService.UnauthenticatedToken,
          isFireAndForgetMethod = false)

        Await.result(
          sendRequest[TransactionService.Authenticate.Args, TransactionService.Authenticate.Result, Int](
            message,
            Protocol.Authenticate,
            _.success.getOrElse(throw new AuthenticationException("Authentication key is incorrect.")))(context),
          connectionOptions.connectionTimeoutMs.millis)
      } else {
        maybeFailCause.get match {
          case Some(exception) => throw exception
          case None =>
            Thread.sleep(connectionOptions.connectionTimeoutMs)
            inner()
        }
      }
    }

    Try(inner()) match {
      case Success(i) => i
      case Failure(exception) =>
        setFailCause(exception)
        shutdown()

        throw exception
    }
  }

  private def getMaxPackagesSizes(): TransportOptionsInfo = {
    if (logger.isInfoEnabled)
      logger.info("getMaxPackagesSizes method is invoked.")

    onShutdownThrowException()

    Await.result(
      methodWithoutMessageSizeValidation[TransactionService.GetMaxPackagesSizes.Args, TransactionService.GetMaxPackagesSizes.Result, TransportOptionsInfo](
        Protocol.GetMaxPackagesSizes,
        TransactionService.GetMaxPackagesSizes.Args(),
        _.success.get)(context),
      connectionOptions.requestTimeoutMs.millis)
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

  private def setFailCause(exception: Throwable): Unit =
    maybeFailCause.compareAndSet(None, Some(exception))
}
