package com.bwsw.tstreams.coordination.client

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.coordination.messages.master._
import com.bwsw.tstreams.coordination.messages.state.TransactionStateMessage
import com.bwsw.tstreams.coordination.server.RequestsTcpServer
import io.netty.channel.{Channel, ChannelHandler, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.util.ReferenceCountUtil

/**
  * Transport implementation
  */
class TcpTransport(address: String, timeoutMs: Int, retryCount: Int = 3, retryDelayMs: Int = 5000) {
  val isIgnore = new AtomicBoolean(false)
  var callback: (Channel,String) => Unit = null

  def getInetAddress() = address

  @ChannelHandler.Sharable
  class ChannelHandler extends SimpleChannelInboundHandler[String] {
    override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
      if(!isIgnore.get)
        executor.submit(new Runnable {
          override def run(): Unit = {
            callback(ctx.channel, msg)
            ReferenceCountUtil.release(msg)
          }
        })
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
      IMessage.logger.error(cause.getMessage)
    }
  }

  private val executor = new FirstFailLockableTaskExecutor("PeerAgent-TcpTransportExecutor")
  private val splits = address.split(":")
  private val server: RequestsTcpServer = new RequestsTcpServer(splits(0), splits(1).toInt, new ChannelHandler())
  private val client: CommunicationClient = new CommunicationClient(timeoutMs, retryCount, retryDelayMs)

  /**
    * Request to disable concrete master
    *
    * @param to
    * @param partition
    * @return DeleteMasterResponse or null
    */
  def deleteMasterRequest(to: String, partition: Int): IMessage = {
    val response = client.sendAndWaitResponse(DeleteMasterRequest(address, to, partition), isExceptionOnFail = false)
    response
  }

  def getTimeout() = timeoutMs

  /**
    * Request to figure out state of receiver
    *
    * @param to
    * @param partition
    * @return PingResponse or null
    */
  def pingRequest(to: String, partition: Int): IMessage = {
    val response = client.sendAndWaitResponse(PingRequest(address, to, partition), isExceptionOnFail = false)
    response
  }

  /**
    * Request to set concrete master
    *
    * @param to
    * @param partition
    * @return SetMasterResponse or null
    */
  def setMasterRequest(to: String, partition: Int): IMessage = {
    val response: IMessage = client.sendAndWaitResponse(SetMasterRequest(address, to, partition), isExceptionOnFail = false)
    response
  }

  /**
    * Request to get Txn
    *
    * @param to
    * @param partition
    * @return TransactionResponse or null
    */
  def transactionRequest(to: String, partition: Int): IMessage = {
    val response: IMessage = client.sendAndWaitResponse(NewTransactionRequest(address, to, partition), isExceptionOnFail = false)
    response
  }

  /**
    * Request to publish event about Txn
    *
    * @param to
    * @param msg     Message
    */
  def publishRequest(to: String, msg: TransactionStateMessage): Unit = {
    client.sendAndNoWaitResponse(PublishRequest(address, to, msg) , isExceptionOnFail = true)
  }

  /**
    * Request to publish event about Txn
    *
    * @param to
    * @param msg     Message
    */
  def materializeRequest(to: String, msg: TransactionStateMessage): Unit = {
    client.sendAndNoWaitResponse(MaterializeRequest(address, to , msg), isExceptionOnFail = true)
  }

  /**
    * Bind local agent address in transport
    */
  def start(callback: (Channel,String) => Unit): Unit = {
    this.callback = callback
    server.start()
  }

  /**
    * Stop transport listen incoming messages
    */
  def stop(): Unit = {
    IMessage.logger.info(s"Transport is shutting down.")
    client.close()
    server.stop()
    executor.shutdownOrDie(100, TimeUnit.SECONDS)
  }


}