package com.bwsw.tstreamstransactionserver.netty.server.multiNode

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.handler.RequestRouter._
import com.bwsw.tstreamstransactionserver.netty.server.handler.auth.{AuthenticateHandler, IsValidHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.consumer.GetConsumerStateHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.data.GetTransactionDataHandler
import com.bwsw.tstreamstransactionserver.netty.server.handler.metadata._
import com.bwsw.tstreamstransactionserver.netty.server.handler.stream.{CheckStreamExistsHandler, DelStreamHandler, GetStreamHandler, PutStreamHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.transport.{GetMaxPackagesSizesHandler, GetZKCheckpointGroupServerPrefixHandler}
import com.bwsw.tstreamstransactionserver.netty.server.handler.{RequestHandler, RequestRouter}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{BookkeeperMaster, BookkeeperWriter}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.commitLog.GetCommitLogOffsetsHandler
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.consumer.PutConsumerCheckpointHandler
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.data.{PutProducerStateWithDataHandler, PutSimpleTransactionAndDataHandler, PutTransactionDataHandler}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.metadata.{OpenTransactionHandler, PutTransactionHandler, PutTransactionsHandler}
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.OpenedTransactionNotifier
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import com.bwsw.tstreamstransactionserver.netty.server.{OrderedExecutionContextPool, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{AuthenticationOptions, CheckpointGroupRoleOptions, TransportOptions}
import io.netty.channel.ChannelHandlerContext
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler.Util._
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKMasterElector

import scala.collection.Searching.{Found, _}
import scala.concurrent.ExecutionContext

class CommonCheckpointGroupHandlerRouter(server: TransactionServer,
                                         private implicit val bookkeeperWriter: BookkeeperWriter,
                                         commonMaster: BookkeeperMaster,
                                         checkpointMaster: BookkeeperMaster,
                                         masterElectors: Seq[ZKMasterElector],
                                         private implicit val multiNodeCommitLogService: CommitLogService,
                                         packageTransmissionOpts: TransportOptions,
                                         authOptions: AuthenticationOptions,
                                         orderedExecutionPool: OrderedExecutionContextPool,
                                         notifier: OpenedTransactionNotifier,
                                         serverRoleOptions: CheckpointGroupRoleOptions,
                                         executionContext: ServerExecutionContextGrids,
                                         commitLogContext: ExecutionContext)
  extends RequestRouter{

  private implicit val authService: AuthService =
    new AuthService(authOptions)

  private implicit val transportValidator: TransportValidator =
    new TransportValidator(packageTransmissionOpts)

  private val serverWriteContext: ExecutionContext =
    executionContext.serverWriteContext
  private val serverReadContext: ExecutionContext =
    executionContext.serverReadContext

  private val (handlersIDs: Array[Byte], handlers: Array[RequestHandler]) = Array(

    handlerAuth(new GetCommitLogOffsetsHandler(
      multiNodeCommitLogService,
      bookkeeperWriter,
      serverReadContext
    )),

    handlerAuth(new PutStreamHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new CheckStreamExistsHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new GetStreamHandler(
      server,
      serverReadContext
    )),

    handlerAuth(new DelStreamHandler(
      server,
      serverWriteContext
    )),

    handlerAuth(new GetTransactionIDHandler(
      server
    )),
    handlerAuth(new GetTransactionIDByTimestampHandler(
      server
    )),

    handlerAuthMetadata(new PutTransactionHandler(
      commonMaster,
      commitLogContext
    )),

    handlerAuthMetadata(new PutTransactionsHandler(
      checkpointMaster,
      commitLogContext
    )),

    handlerAuthData(new OpenTransactionHandler(
      server,
      commonMaster,
      notifier,
      authOptions,
      orderedExecutionPool,
      commitLogContext
    )),

    handlerReadAuth(new GetTransactionHandler(
      server,
      serverReadContext,
    ), masterElectors),

    handlerReadAuthData(new GetLastCheckpointedTransactionHandler(
      server,
      serverReadContext
    ), masterElectors),

    handlerReadAuth(new ScanTransactionsHandler(
      server,
      serverReadContext
    ), masterElectors),

    handlerAuthData(new PutProducerStateWithDataHandler(
      commonMaster,
      commitLogContext
    )),

    handlerAuthData(new PutSimpleTransactionAndDataHandler(
      server,
      commonMaster,
      notifier,
      authOptions,
      orderedExecutionPool,
      commitLogContext
    )),

    handlerAuthData(new PutTransactionDataHandler(
      commonMaster,
      serverWriteContext
    )),

    handlerReadAuth(new GetTransactionDataHandler(
      server,
      serverReadContext
    ), masterElectors),

    handlerAuthMetadata(new PutConsumerCheckpointHandler(
      commonMaster,
      commitLogContext
    )),
    handlerReadAuth(new GetConsumerStateHandler(
      server,
      serverReadContext
    ), masterElectors),

    handlerId(new AuthenticateHandler(
      authService
    )),
    handlerId(new IsValidHandler(
      authService
    )),

    handlerId(new GetMaxPackagesSizesHandler(
      packageTransmissionOpts
    )),

    handlerId(new GetZKCheckpointGroupServerPrefixHandler(
      serverRoleOptions
    ))
  ).sortBy(_._1).unzip



  override def route(message: RequestMessage,
                     ctx: ChannelHandlerContext): Unit = {
    handlersIDs.search(message.methodId) match {
      case Found(index) =>
        val handler = handlers(index)
        handler.handle(message, ctx, None)
      case _ =>
      //        throw new IllegalArgumentException(s"Not implemented method that has id: ${message.methodId}")
    }
  }
}
