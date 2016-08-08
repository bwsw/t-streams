package com.bwsw.tstreams.coordination.messages.master

import java.util.UUID

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

  def handleP2PRequest(agent: PeerAgent) = {
  }
}

case class TransactionRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: PeerAgent) = {
    assert(receiverID == agent.getAgentAddress)
    if (agent.localMasters.contains(partition) && agent.localMasters(partition) == agent.getAgentAddress) {
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

case class TransactionResponse(senderID: String, receiverID: String, txnUUID: UUID, partition: Int) extends IMessage

case class DeleteMasterRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: PeerAgent) = {
    assert(receiverID == agent.getAgentAddress)
    val response = {
      if (agent.localMasters.contains(partition) && agent.localMasters(partition) == agent.getAgentAddress) {
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

case class DeleteMasterResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

case class SetMasterRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: PeerAgent) = {
    assert(receiverID == agent.getAgentAddress)
    val response = {
      if (agent.localMasters.contains(partition) && agent.localMasters(partition) == agent.getAgentAddress)
        EmptyResponse(receiverID, senderID, partition)
      else {
        agent.localMasters(partition) = agent.getAgentAddress
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

case class SetMasterResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

case class PingRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: PeerAgent) = {
    assert(receiverID == agent.getAgentAddress)
    val response = {
      if (agent.localMasters.contains(partition) && agent.localMasters(partition) == agent.getAgentAddress)
        PingResponse(receiverID, senderID, partition)
      else
        EmptyResponse(receiverID, senderID, partition)
    }
    response.msgID = msgID
    agent.getTransport.respond(response)
  }
}

case class PingResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

case class PublishRequest(senderID: String, receiverID: String, msg: Message) extends IMessage {
  override val partition: Int = msg.partition

  override def handleP2PRequest(agent: PeerAgent) = {
    assert(receiverID == agent.getAgentAddress)
    if (agent.localMasters.contains(msg.partition) && agent.localMasters(msg.partition) == agent.getAgentAddress) {
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

case class PublishResponse(senderID: String, receiverID: String, msg: Message) extends IMessage {
  override val partition: Int = msg.partition
}


case class EmptyRequest(senderID: String, receiverID: String, partition: Int) extends IMessage {
  override def handleP2PRequest(agent: PeerAgent) = {
    val response = EmptyResponse(receiverID, senderID, partition)
    response.msgID = msgID
    agent.getTransport.respond(response)
  }
}

case class EmptyResponse(senderID: String, receiverID: String, partition: Int) extends IMessage

