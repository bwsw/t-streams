package com.bwsw.tstreams.coordination.messages.master

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.common.LockUtil
import com.bwsw.tstreams.coordination.messages.state.{TransactionStatus, Message}
import com.bwsw.tstreams.coordination.producer.p2p.PeerAgent

import scala.util.Random

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
    * @param agent
    */
  def handleP2PRequest(agent: PeerAgent) = {
  }
}

/**
  * Message which is received when producer requests new transaction
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class TransactionRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: PeerAgent) = {
    assert(receiverID == agent.getAgentAddress)
    val master = agent.localMasters.getOrDefault(partition, "")
    if (master == agent.getAgentAddress) {
      val txnUUID: UUID = agent.getProducer.getNewTxnUUIDLocal()
      agent.getProducer.openTxnLocal(txnUUID, partition,
        onComplete = () => {
          val response = TransactionResponse(receiverID, senderID, txnUUID, partition)
          response.msgID = msgID
          agent.getTransport.respond(response)
        })
    } else {
      val response = EmptyResponse(receiverID, senderID, partition)
      response.msgID = msgID
      agent.getTransport.respond(response)
    }
  }
}

/**
  * response on new transaction
  * @param senderID
  * @param receiverID
  * @param txnUUID
  * @param partition
  */
case class TransactionResponse(senderID: String, receiverID: String, txnUUID: UUID, partition: Int) extends IMessage

/**
  * Message which is received when due to voting master must be revoked from current agent for certain partition
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class DeleteMasterRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: PeerAgent) = {
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
  }
}

/**
  * Response message on master revokation from this agent for certain partition
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class DeleteMasterResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

/**
  * Request message to assign master for certain partition at this agent
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class SetMasterRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: PeerAgent) = {
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
  }
}

/**
  * response on master assignment
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class SetMasterResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

/**
  * Ping/Pong request (keep alive)
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class PingRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: PeerAgent) = {
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
  }
}

/**
  * Ping/Pong response (keep alive)
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class PingResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

/**
  * Request which is received when producer does publish operation thru master and master proxies it to subscribers
  * @param senderID
  * @param receiverID
  * @param msg
  */
case class PublishRequest(senderID: String, receiverID: String, msg: Message) extends IMessage {
  override val partition: Int = msg.partition

  override def handleP2PRequest(agent: PeerAgent) = {
    assert(receiverID == agent.getAgentAddress)
    val master = agent.localMasters.getOrDefault(partition, "")
    if(master == agent.getAgentAddress) {
      agent.getProducer.subscriberNotifier.publish(msg,
        onComplete = () => {
          val response = PublishResponse(
            senderID = receiverID,
            receiverID = senderID,
            msg = Message(UUID.randomUUID(), 0, TransactionStatus.opened, msg.partition))
          response.msgID = msgID
          agent.getTransport.respond(response)
        })
    } else {
      val response = EmptyResponse(receiverID, senderID, msg.partition)
      response.msgID = msgID
      agent.getTransport.respond(response)
    }
  }
}

/**
  * Publish response is sent to producer to acknowledge it that request is accepted
  * @param senderID
  * @param receiverID
  * @param msg
  */
case class PublishResponse(senderID: String, receiverID: String, msg: Message) extends IMessage {
  override val partition: Int = msg.partition
}

/**
  * Just empty dump request
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class EmptyRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: PeerAgent) = {
    val response = EmptyResponse(receiverID, senderID, partition)
    response.msgID = msgID
    agent.getTransport.respond(response)
  }
}

/**
  * Response on empty request
  * @param senderID
  * @param receiverID
  * @param partition
  */
case class EmptyResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

