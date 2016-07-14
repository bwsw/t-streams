//package com.bwsw.tstreams.coordination.transactions.peertopeer
//
//import java.util.UUID
//import java.util.concurrent.locks.ReentrantLock
//import java.util.concurrent.{ExecutorService, Executors}
//
//import com.bwsw.tstreams.agents.producer.BasicProducer
//import com.bwsw.tstreams.common.zkservice.ZkService
//import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
//import com.bwsw.tstreams.coordination.transactions.messages.{EmptyRequest, PublishRequest, PublishResponse, _}
//import com.bwsw.tstreams.coordination.transactions.transport.traits.ITransport
//import org.apache.zookeeper.CreateMode
//import org.slf4j.LoggerFactory
//
//
//class PartitionHandler(producer: BasicProducer[_,_],
//                       partitions : List[Int],
//                       agentAddress : String,
//                       transport : ITransport,
//                       zkService: ZkService) {
//
//  private val logger = LoggerFactory.getLogger(this.getClass)
//  private val streamName = producer.stream.getName
//  private val executor: ExecutorService = Executors.newSingleThreadExecutor()
//  private val partitionToMaster = scala.collection.mutable.Map[Int, String]()
//  private val lockPartitionToMaster = new ReentrantLock(true)
//  private val lockManagingMaster = new ReentrantLock(true)
//
//  def executeTask(msg : IMessage) : Unit = {
//    assert(partitions.contains(msg.partition))
//    val task = createTask(msg)
//    executor.execute(task)
//  }
//
//  /**
//    * Create task to handle IMessage message
//    *
//    * @param request Requested message
//    */
//  private def createTask(request : IMessage): Runnable = {
//    new Runnable {
//      override def run(): Unit = {
//        request match {
//          case PingRequest(snd, rcv, partition) =>
//            lockPartitionToMaster.lock()
//            assert(rcv == agentAddress)
//            val response = {
//              if (partitionToMaster.contains(partition) && partitionToMaster(partition) == agentAddress)
//                PingResponse(rcv, snd, partition)
//              else
//                EmptyResponse(rcv, snd, partition)
//            }
//            lockPartitionToMaster.unlock()
//            response.msgID = request.msgID
//            transport.response(response)
//
//          case SetMasterRequest(snd, rcv, partition) =>
//            lockPartitionToMaster.lock()
//            assert(rcv == agentAddress)
//            val response = {
//              if (partitionToMaster.contains(partition) && partitionToMaster(partition) == agentAddress)
//                EmptyResponse(rcv,snd,partition)
//              else {
//                partitionToMaster(partition) = agentAddress
//                setThisAgentAsMaster(partition)
//                usedPartitions foreach { partition =>
//                  updateThisAgentPriority(partition, value = -1)
//                }
//                SetMasterResponse(rcv, snd, partition)
//              }
//            }
//            lockPartitionToMaster.unlock()
//            response.msgID = request.msgID
//            transport.response(response)
//
//          case DeleteMasterRequest(snd, rcv, partition) =>
//            lockPartitionToMaster.lock()
//            assert(rcv == agentAddress)
//            val response = {
//              if (partitionToMaster.contains(partition) && partitionToMaster(partition) == agentAddress) {
//                partitionToMaster.remove(partition)
//                deleteThisAgentFromMasters(partition)
//                usedPartitions foreach { partition =>
//                  updateThisAgentPriority(partition, value = 1)
//                }
//                DeleteMasterResponse(rcv, snd, partition)
//              } else
//                EmptyResponse(rcv, snd, partition)
//            }
//            lockPartitionToMaster.unlock()
//            response.msgID = request.msgID
//            transport.response(response)
//
//          case TransactionRequest(snd, rcv, partition) =>
//            lockPartitionToMaster.lock()
//            assert(rcv == agentAddress)
//            val response = {
//              if (partitionToMaster.contains(partition) && partitionToMaster(partition) == agentAddress) {
//                val txnUUID: UUID = producer.getLocalTxn(partition)
//                TransactionResponse(rcv, snd, txnUUID, partition)
//              } else
//                EmptyResponse(rcv, snd, partition)
//            }
//            lockPartitionToMaster.unlock()
//            response.msgID = request.msgID
//            transport.response(response)
//
//          case PublishRequest(snd, rcv, msg) =>
//            lockPartitionToMaster.lock()
//            assert(rcv == agentAddress)
//            val response = {
//              if (partitionToMaster.contains(msg.partition) && partitionToMaster(msg.partition) == agentAddress) {
//                producer.coordinator.publish(msg)
//                PublishResponse(rcv, snd,
//                  ProducerTopicMessage(UUID.randomUUID(),0,ProducerTransactionStatus.opened,msg.partition))
//              } else
//                EmptyResponse(rcv, snd, msg.partition)
//            }
//            lockPartitionToMaster.unlock()
//            response.msgID = request.msgID
//            transport.response(response)
//
//          case EmptyRequest(snd,rcv,p) =>
//            val response = EmptyResponse(rcv,snd,p)
//            response.msgID = request.msgID
//            transport.response(response)
//        }
//      }
//    }
//  }
//
//  /**
//    * Return master for concrete partition
//    *
//    * @param partition Partition to set
//    * @return Master address
//    */
//  private def getMaster(partition : Int) : Option[String] = {
//    //lock to guarantee that in one moment will be executed exactly one operation
//    //getMaster, setThisAgentAsMaster, deleteThisAgentFromMasters
//    lockManagingMaster.lock()
//    //lock to guarantee that nobody will change master in moment of getting it
//    val lock = zkService.getLock(s"/producers/lock_master/$streamName/$partition")
//    lock.lock()
//    val masterOpt = zkService.get[String](s"/producers/master/$streamName/$partition")
//    lock.unlock()
//    lockManagingMaster.unlock()
//    logger.debug(s"[GET MASTER]Agent:{${masterOpt.getOrElse("None")}} is current master on" +
//      s" stream:{$streamName},partition:{$partition}\n")
//    masterOpt
//  }
//
//  /**
//    * Set this agent as new master on concrete partition
//    *
//    * @param partition Partition to set
//    */
//  private def setThisAgentAsMaster(partition : Int) : Unit = {
//    lockManagingMaster.lock()
//    val lock = zkService.getLock(s"/producers/lock_master/$streamName/$partition")
//    lock.lock()
//    //TODO remove after debug
//    assert(!zkService.exist(s"/producers/master/$streamName/$partition"))
//    zkService.create(s"/producers/master/$streamName/$partition", agentAddress, CreateMode.EPHEMERAL)
//    lock.unlock()
//    lockManagingMaster.unlock()
//    logger.debug(s"[SET MASTER]Agent:{$agentAddress} in master now on" +
//      s" stream:{$streamName},partition:{$partition}\n")
//  }
//
//  /**
//    * Unset this agent as master on concrete partition
//    *
//    * @param partition Partition to set
//    */
//  private def deleteThisAgentFromMasters(partition : Int) : Unit = {
//    lockManagingMaster.lock()
//    val lock = zkService.getLock(s"/producers/lock_master/$streamName/$partition")
//    lock.lock()
//    zkService.delete(s"/producers/master/$streamName/$partition")
//    lock.unlock()
//    lockManagingMaster.unlock()
//    logger.debug(s"[DELETE MASTER]Agent:{$agentAddress} in NOT master now on" +
//      s" stream:{$streamName},partition:{$partition}\n")
//  }
//}