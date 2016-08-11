package com.bwsw.tstreams.coordination.messages.master

import java.util.UUID

import com.bwsw.tstreams.common.TimeTracker
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus._
import com.bwsw.tstreams.coordination.messages.state.{TransactionStatus, Message}
import com.bwsw.tstreams.coordination.producer.p2p.PeerAgent
import org.slf4j.LoggerFactory

import scala.util.Random

object IMessage {
  val logger = LoggerFactory.getLogger(this.getClass)
}

/**
  * Messages which used for providing
  * interaction between [[com.bwsw.tstreams.coordination.producer.p2p.PeerAgent]]]
  */
trait IMessage {
  var msgID: Long = Random.nextLong()
  val senderID: String
  val receiverID: String
  val partition: Int

  /**
    * Called by PeerAgent if message is received
    *
    * @param agent
    */
  def handleP2PRequest(agent: PeerAgent) = {}

  override def toString(): String = {
    s"Type: ${this.getClass}\nID: ${msgID}\nSender: ${senderID}\nReceiver: ${receiverID}\nPartition: ${partition}"
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
    IMessage.logger.debug("Start handling NewTransactionRequest")
    assert(receiverID == agent.getAgentAddress)
    val master = agent.localMasters.getOrDefault(partition, "")
    if (master == agent.getAgentAddress) {
      val txnUUID: UUID = agent.getProducer.getNewTxnUUIDLocal()
      val response = TransactionResponse(receiverID, senderID, txnUUID, partition)
      response.msgID = msgID
      agent.getTransport.respond(response)
      IMessage.logger.debug(s"Responded with early ready virtualized TXN: ${txnUUID}")
      agent.submitPipelinedTask(new Runnable {
          def run(): Unit = agent.getProducer.openTxnLocal(txnUUID, partition,
              onComplete = () => {
                  agent.notifyMaterialize(
                      Message(txnUUID, -1, TransactionStatus.materialize, partition), senderID)
                IMessage.logger.debug(s"Responded with complete ready TXN: ${txnUUID}")
              })
        }, partition)
    } else {
      val response = EmptyResponse(receiverID, senderID, partition)
      response.msgID = msgID
      agent.getTransport.respond(response)
    }
  }
}

/**
  * response on new transaction
  *
  * @param senderID
  * @param receiverID
  * @param txnUUID
  * @param partition
  */
case class TransactionResponse(senderID: String, receiverID: String, txnUUID: UUID, partition: Int) extends IMessage

/**
  * Message which is received when due to voting master must be revoked from current agent for certain partition
  *
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class DeleteMasterRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: PeerAgent) = {
    IMessage.logger.debug("Start handling DeleteMasterRequest")
    assert(receiverID == agent.getAgentAddress)
    val master = agent.localMasters.getOrDefault(partition, "")
    val response = {
      if (master == agent.getAgentAddress) {
        agent.localMasters.remove(partition)
        agent.deleteThisAgentFromMasters(partition)
        agent.getUsedPartitions foreach { partition =>
          agent.updateThisAgentPriority(partition, value = 1)
        }
        DeleteMasterResponse(receiverID, senderID, partition)
      } else
        EmptyResponse(receiverID, senderID, partition)
    }
    response.msgID = msgID
    agent.getTransport.respond(response)
    IMessage.logger.debug(s"sEnd handling DeleteMasterRequest ${response}")
  }
}

/**
  * Response message on master revokation from this agent for certain partition
  *
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class DeleteMasterResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

/**
  * Request message to assign master for certain partition at this agent
  *
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class SetMasterRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: PeerAgent) = {
    IMessage.logger.debug("Start handling SetMasterRequest")
    assert(receiverID == agent.getAgentAddress)
    val master = agent.localMasters.getOrDefault(partition, "")
    val response = {
      if (master == agent.getAgentAddress)
        EmptyResponse(receiverID, senderID, partition)
      else {
        agent.localMasters.put(partition, agent.getAgentAddress)
        agent.setThisAgentAsMaster(partition)
        agent.getUsedPartitions foreach { partition =>
          agent.updateThisAgentPriority(partition, value = -1)
        }
        SetMasterResponse(receiverID, senderID, partition)
      }
    }
    response.msgID = msgID
    agent.getTransport.respond(response)
    IMessage.logger.debug(s"End handling SetMasterRequest ${response}")
  }
}

/**
  * response on master assignment
  *
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class SetMasterResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

/**
  * Ping/Pong request (keep alive)
  *
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class PingRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: PeerAgent) = {
    IMessage.logger.debug("Start handling PingRequest")
    assert(receiverID == agent.getAgentAddress)
    val master = agent.localMasters.getOrDefault(partition, "")
    val response = {
      if (master == agent.getAgentAddress)
        PingResponse(receiverID, senderID, partition)
      else
        EmptyResponse(receiverID, senderID, partition)
    }
    response.msgID = msgID
    agent.getTransport.respond(response)
    IMessage.logger.debug(s"End handling SetMasterRequest: ${response}")
  }
}

/**
  * Ping/Pong response (keep alive)
  *
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class PingResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

/**
  * Request which is received when producer does publish operation thru master and master proxies it to subscribers
  *
  * @param senderID
  * @param receiverID
  * @param msg
  */
case class PublishRequest(senderID: String, receiverID: String, msg: Message) extends IMessage {
  override val partition: Int = msg.partition

  override def handleP2PRequest(agent: PeerAgent) = {
    IMessage.logger.debug("Start handling PublishRequest")
    assert(receiverID == agent.getAgentAddress)
    val master = agent.localMasters.getOrDefault(partition, "")
    if(master == agent.getAgentAddress) {
      agent.submitPipelinedTask(new Runnable {
        override def run(): Unit = agent.getProducer.subscriberNotifier.publish(msg, onComplete = () => {})
      }, partition)
    }
    IMessage.logger.debug("End handling PublishRequest")
  }
}

/**
  * Publish response is sent to producer to acknowledge it that request is accepted
  *
  * @param senderID
  * @param receiverID
  * @param msg
  */
case class PublishResponse(senderID: String, receiverID: String, msg: Message) extends IMessage {
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
case class MaterializeRequest(senderID: String, receiverID: String, msg: Message) extends IMessage {
  override val partition: Int = msg.partition
  override def handleP2PRequest(agent: PeerAgent) = {
   agent.getProducer.materialize(msg)
  }
}