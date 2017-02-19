package com.bwsw.tstreams.coordination.messages.master

import com.bwsw.tstreams.agents.producer
import com.bwsw.tstreams.agents.producer.PeerAgent
import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.coordination.messages.state.{TransactionStateMessage, TransactionStatus}
import io.netty.channel.Channel
import org.slf4j.LoggerFactory

import scala.util.Random


object IMessage {
  val logger = LoggerFactory.getLogger(this.getClass)
  val isDebugMessages = false
}

/**
  * Messages which used for providing
  * interaction between [[producer.PeerAgent]]]
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
  def handleP2PRequest(agent: PeerAgent) = {}

  def run(agent: PeerAgent) = {
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
  override def handleP2PRequest(agent: PeerAgent) = {
    assert(receiverID == agent.getAgentAddress)

    val master = agent.isMasterOfPartition(partition)

    if (master) {
      val transactionID = agent.getProducer.generateNewTransactionIDLocal()
      val response = TransactionResponse(receiverID, senderID, transactionID, partition)
      response.msgID = msgID
      this.respond(response)

      if (IMessage.logger.isDebugEnabled)
        IMessage.logger.debug(s"Responded with early ready virtual transaction: $transactionID")

      agent.getProducer.openTransactionLocal(transactionID, partition,
        onComplete = () => {
          agent.notifyMaterialize(TransactionStateMessage(transactionID, -1, TransactionStatus.materialize, partition, agent.getUniqueAgentID(), -1, 0), senderID)

          if (IMessage.logger.isDebugEnabled)
            IMessage.logger.debug(s"Responded with complete ready transaction: $transactionID")
        })

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
  * Request which is received when producer does publish operation through master and master proxies it to subscribers
  *
  * @param senderID
  * @param receiverID
  * @param msg
  */
case class PublishRequest(senderID: String, receiverID: String, msg: TransactionStateMessage) extends IMessage {
  override val partition: Int = msg.partition

  override def handleP2PRequest(agent: PeerAgent) = {
    if (agent.isMasterOfPartition(partition)) {
      agent.submitPipelinedTaskToPublishExecutors(partition,
        () => agent.getProducer.subscriberNotifier.publish(msg))
    }
  }
}

/**
  * Publish response is sent to producer to acknowledge it that request is accepted
  *
  * @param senderID
  * @param receiverID
  * @param msg
  */
case class PublishResponse(senderID: String, receiverID: String, msg: TransactionStateMessage) extends IMessage {
  override val partition: Int = msg.partition
}

/**
  * Just empty dump request
  *
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class EmptyRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
}

/**
  * Response on empty request
  *
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class EmptyResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

/**
  * Just empty dump request
  *
  * @param senderID
  * @param receiverID
  * @param msg
  */
case class MaterializeRequest(senderID: String, receiverID: String, msg: TransactionStateMessage) extends IMessage {
  override val partition: Int = msg.partition

  override def handleP2PRequest(agent: PeerAgent) = {
    agent.getProducer.materialize(msg)
  }
}