package com.bwsw.tstreamstransactionserver.netty.server.multiNode.handler

import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.handler._
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperWriter
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.transportService.TransportValidator
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKMasterElector

object Util {
  final def handlerReadAuthData(clientRequestHandler: ClientRequestHandler,
                                zKMasterElectors: Seq[ZKMasterElector])
                               (implicit
                                authService: AuthService,
                                transportValidator: TransportValidator,
                                commitLogService: CommitLogService,
                                bookkeeperWriter: BookkeeperWriter): (Byte, RequestHandler) = {
    val handler =
      new ReadAllowedOperationHandler(
        new DataPackageSizeValidationHandler(
          clientRequestHandler,
          transportValidator
        ),
        commitLogService,
        bookkeeperWriter,
      )
    zKMasterElectors.foreach(_.addLeaderListener(handler))

    val id = clientRequestHandler.id
    id -> new AuthHandler(
      handler,
      authService
    )
  }

  final def handlerReadAuthMetadata(clientRequestHandler: ClientRequestHandler,
                                    zKMasterElectors: Seq[ZKMasterElector])
                                   (implicit
                                    authService: AuthService,
                                    transportValidator: TransportValidator,
                                    commitLogService: CommitLogService,
                                    bookkeeperWriter: BookkeeperWriter): (Byte, RequestHandler) = {
    val handler =
      new ReadAllowedOperationHandler(
        new MetadataPackageSizeValidationHandler(
          clientRequestHandler,
          transportValidator
        ),
        commitLogService,
        bookkeeperWriter,
      )
    zKMasterElectors.foreach(_.addLeaderListener(handler))

    val id = clientRequestHandler.id
    id -> new AuthHandler(
      handler,
      authService
    )
  }

  final def handlerReadAuth(clientRequestHandler: ClientRequestHandler,
                            zKMasterElectors: Seq[ZKMasterElector])
                           (implicit
                            authService: AuthService,
                            commitLogService: CommitLogService,
                            bookkeeperWriter: BookkeeperWriter): (Byte, RequestHandler) = {
    val handler =
      new ReadAllowedOperationHandler(
        clientRequestHandler,
        commitLogService,
        bookkeeperWriter
      )
    zKMasterElectors.foreach(_.addLeaderListener(handler))

    val id = clientRequestHandler.id
    id -> new AuthHandler(
      handler,
      authService
    )
  }
}
