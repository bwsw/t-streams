package com.bwsw.tstreamstransactionserver.netty.server.handler

abstract class IntermediateRequestHandler(nextHandler: RequestHandler)
  extends RequestHandler
