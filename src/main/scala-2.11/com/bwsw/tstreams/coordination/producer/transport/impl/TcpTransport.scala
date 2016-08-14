package com.bwsw.tstreams.coordination.producer.transport.impl

import java.util.concurrent.Executors

import com.bwsw.tstreams.coordination.messages.master._
import com.bwsw.tstreams.coordination.producer.transport.impl.client.InterProducerCommunicationClient
import com.bwsw.tstreams.coordination.producer.transport.impl.server.ProducerRequestsTcpServer
import com.bwsw.tstreams.coordination.producer.transport.traits.ITransport
import io.netty.channel.{Channel, ChannelHandler, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.util.ReferenceCountUtil
import org.slf4j.LoggerFactory

/**
  * [[ITransport]] implementation
  */
class TcpTransport(address: String, timeoutMs: Int, retryCount: Int = 3, retryDelayMs: Int = 5000) extends ITransport {

  var callback: (Channel,String) => Unit = null

  @ChannelHandler.Sharable
  class ChannelHandler extends SimpleChannelInboundHandler[String] {
    override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
      executor.submit(new Runnable {
        override def run(): Unit = {
          callback(ctx.channel, msg)
          ReferenceCountUtil.release(msg)
        }
      })
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
      println(cause.getCause)
    }
  }

  private val executor = Executors.newSingleThreadExecutor()
  private val splits = address.split(":")
  private val server: ProducerRequestsTcpServer = new ProducerRequestsTcpServer(splits(0), splits(1).toInt, new ChannelHandler())
  private val client: InterProducerCommunicationClient = new InterProducerCommunicationClient(timeoutMs, retryCount, retryDelayMs)

  /**
    * Request to disable concrete master
    *
    * @param msg     Msg to disable master
    * @return DeleteMasterResponse or null
    */
  override def deleteMasterRequest(msg: DeleteMasterRequest): IMessage = {
    val response = client.sendAndWaitResponse(msg, isExceptionOnFail = false)
    response
  }

  override def getTimeout() = timeoutMs

  /**
    * Request to figure out state of receiver
    *
    * @param msg Message
    * @return PingResponse or null
    */
  override def pingRequest(msg: PingRequest): IMessage = {
    val response = client.sendAndWaitResponse(msg, isExceptionOnFail = false)
    response
  }

  /**
    * Send empty request (just for testing)
    *
    * @param msg EmptyRequest
    */
  override def stopRequest(msg: EmptyRequest): Unit = {
    val logger = LoggerFactory.getLogger(this.getClass)
    client.sendAndNoWaitResponse(msg, isExceptionOnFail = true)
  }

  /**
    * Request to set concrete master
    *
    * @param msg     Message
    * @return SetMasterResponse or null
    */
  override def setMasterRequest(msg: SetMasterRequest): IMessage = {
    val response: IMessage = client.sendAndWaitResponse(msg, isExceptionOnFail = false)
    response
  }

  /**
    * Request to get Txn
    *
    * @param msg     Message
    * @return TransactionResponse or null
    */
  override def transactionRequest(msg: NewTransactionRequest): IMessage = {
    val start = System.currentTimeMillis()
    val response: IMessage = client.sendAndWaitResponse(msg, isExceptionOnFail = false)
    val delaySvr = response.remotePeerTimestamp - msg.remotePeerTimestamp
    val end = System.currentTimeMillis()
    if(IMessage.logger.isDebugEnabled)
      IMessage.logger.debug(s"Server view: ${delaySvr}, client view: ${end - start}")
    response
  }

  /**
    * Request to publish event about Txn
    *
    * @param msg     Message
    */
  override def publishRequest(msg: PublishRequest): Unit = {
    client.sendAndNoWaitResponse(msg, isExceptionOnFail = true)
  }

  /**
    * Request to publish event about Txn
    *
    * @param msg     Message
    */
  override def materializeRequest(msg: MaterializeRequest): Unit = {
    client.sendAndNoWaitResponse(msg, isExceptionOnFail = true)
  }

  /**
    * Bind local agent address in transport
    */
  override def start(callback: (Channel,String) => Unit): Unit = {
    this.callback = callback
    server.start()
  }

  /**
    * Stop transport listen incoming messages
    */
  override def stop(): Unit = {
    IMessage.logger.warn("Transport is shutting down.")
    client.close()
    server.stop()
  }


}