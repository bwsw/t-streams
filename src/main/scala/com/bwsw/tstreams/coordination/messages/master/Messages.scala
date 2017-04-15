package com.bwsw.tstreams.coordination.messages.master

import com.bwsw.tstreams.agents.producer
import com.bwsw.tstreams.agents.producer.TransactionOpenerService
import com.bwsw.tstreams.common.ProtocolMessageSerializer
import io.netty.channel.Channel
import org.slf4j.LoggerFactory

import scala.util.Random


object IMessage {
  val logger = LoggerFactory.getLogger(this.getClass)
  val isDebugMessages = false
}

/**
  * Messages which used for providing
  * interaction between [[producer.TransactionOpenerService]]]
  */
trait IMessage {
  var msgID: Long = Random.nextLong()
  val senderID: String
  val receiverID: String
  val partition: Int
  var remotePeerTimestamp: Long = System.currentTimeMillis()
  val localPeerTimestamp: Long = System.currentTimeMillis()
  var channel: Channel = null

  def respond(m: IMessage): Unit = {
    val s: String = ProtocolMessageSerializer.wrapMsg(ProtocolMessageSerializer.serialize(m))
    channel.writeAndFlush(s)
  }

  /**
    * Called by PeerAgent if message is received
    *
    * @param agent
    */
  def handleP2PRequest(agent: TransactionOpenerService) = {}

  def run(agent: TransactionOpenerService) = {
    val start = System.currentTimeMillis()
    if (IMessage.isDebugMessages) {
      IMessage.logger.info(s"${getClass.toString} / ${msgID.toString} - Start handling at $start, was-sent-at $remotePeerTimestamp, re-created $localPeerTimestamp")
      IMessage.logger.info(s"${getClass.toString} / ${msgID.toString} - Waiting to be run time: ${start - remotePeerTimestamp}")
    }

    handleP2PRequest(agent)

    if (IMessage.isDebugMessages) {
      IMessage.logger.info(s"${getClass.toString} / ${msgID.toString} - Execution delta: ${System.currentTimeMillis() - start}")
    }
  }

  override def toString(): String = {
    s"Type: ${this.getClass} ID: $msgID Sender: $senderID Receiver: $receiverID Partition: $partition"
  }
}

/**
  * Message which is received when producer requests new transaction
  *
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class NewTransactionRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: TransactionOpenerService) = {
    assert(receiverID == agent.getAgentAddress)

    val master = agent.isMasterOfPartition(partition)

    if (master) {
      val transactionID = agent.getProducer.generateNewTransactionIDLocal()
      val response = TransactionResponse(receiverID, senderID, transactionID, partition)
      response.msgID = msgID
      agent.getProducer().openTransactionLocal(transactionID, partition)
      this.respond(response)
    } else {
      val response = EmptyResponse(receiverID, senderID, partition)
      response.msgID = msgID
      this.respond(response)
    }
  }
}

/**
  * response on new transaction
  *
  * @param senderID
  * @param receiverID
  * @param transactionID
  * @param partition
  */
case class TransactionResponse(senderID: String, receiverID: String, transactionID: Long, partition: Int) extends IMessage

/**
  * Response on empty request
  *
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class EmptyResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

