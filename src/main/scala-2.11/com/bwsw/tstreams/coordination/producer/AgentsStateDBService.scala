package com.bwsw.tstreams.coordination.producer

import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.common.{LockUtil, ZookeeperDLMService}
import org.apache.zookeeper.{CreateMode, KeeperException, Watcher}

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 17.08.16.
  */
class AgentsStateDBService(dlm: ZookeeperDLMService,
                           inetAddress: String,
                           streamName: String,
                           partitions: Set[Int]) {


  def getDLM() = dlm

  private val masterMap = mutable.Map[Int, MasterConfiguration]()
  var agentID: Int = 0

  /**
    * Returns local information about partition master
    *
    * @param partition
    * @param default
    * @return
    */
  def getPartitionMasterInetAddressLocal(partition: Int, default: String): String = this.synchronized {
    val opt = masterMap get partition
    if (opt.isDefined)
      opt.get.agentAddress
    else
      default
  }

  def getCurrentMasterLocal(partition: Int): Option[MasterConfiguration] = this.synchronized {
    masterMap get partition
  }


  def putPartitionMasterLocally(partition: Int, agent: MasterConfiguration) = this.synchronized {
    masterMap += (partition -> agent)
  }

  def removePartitionMasterLocally(partition: Int) = this.synchronized {
    masterMap -= partition
  }




  /**
    * Receives current master without locking partition
    *
    * @param partition
    * @return
    */
  def getCurrentMaster(partition: Int): Option[MasterConfiguration] = {
    val masterOpt = try {
      dlm.get[MasterConfiguration](getPartitionMasterPath(partition))
    } catch {
      case e: Exception =>
        None
    }
    if (PeerAgent.logger.isDebugEnabled) {
      PeerAgent.logger.debug(s"[REQUEST CURRENT MASTER] Agent: ${masterOpt.getOrElse("None")} is current master on stream: {$streamName}, partition: {$partition}.")
    }
    masterOpt
  }

  /**
    * Does bootstrap for agent
    *
    * @param isLowPriorityToBeMaster
    * @param uniqueAgentId
    */
  def bootstrap(isLowPriorityToBeMaster: Boolean, uniqueAgentId: Int, isFull: Boolean) = this.synchronized {
    agentID = uniqueAgentId
    // save initial records to zk
    partitions foreach { p =>
      val penalty = if (isLowPriorityToBeMaster) PeerAgent.LOW_PRIORITY_PENALTY else 0
      val conf = AgentConfiguration(inetAddress, weight = 0, penalty, uniqueAgentId)
      //println(s"ConfInit: $conf")
      dlm.create[AgentConfiguration](getMyPath(p), conf, CreateMode.EPHEMERAL)
    }
    if (PeerAgent.logger.isDebugEnabled) {
      PeerAgent.logger.debug(s"[INIT] End initialize agent with address:{$inetAddress}, " +
        s"stream: {$streamName}, partitions: {${partitions.mkString(",")}")
    }

    removeLastSessionArtifacts()
    // assign me as a master on partitions which don't have master yet
    if (isFull)
      bootstrapBrokenPartitionsMasters()
  }

  private def bootstrapBrokenPartitionsMasters(): Unit = {
    var ctr: Int = 0
    partitions foreach {
      p => if (getCurrentMaster(p).isEmpty) {
        val mc = MasterConfiguration(inetAddress, agentID)
        dlm.create[MasterConfiguration](
          getPartitionMasterPath(p),
          mc,
          CreateMode.EPHEMERAL)
        ctr += 1
        masterMap += (p -> mc)
      }
    }
    partitions foreach { p => updateMyPriority(p, value = ctr) }
  }

  /**
    * removes artifacts
    */
  def shutdown() = this.synchronized {

    val parts = masterMap.keys
    parts foreach { p => demoteMeAsMaster(partition = p, isUpdatePriority = false) }

    partitions foreach { p =>
      dlm.delete(getMyPath(p))
    }
  }

  /**
    * Amend agent priority
    *
    * @param partition Partition to update priority
    * @param value     Value which will be added to current priority
    */
  private def updateMyPriority(partition: Int, value: Int) = {
    PeerAgent.logger.debug(s"[PRIORITY CHANGE BEGIN] Start amend agent priority with value:{$value} with address: {$inetAddress} on stream: {$streamName}, partition: {$partition}")
    updateMySettings(partition, (s: AgentConfiguration) => s.weight += value)
    PeerAgent.logger.debug(s"[PRIORITY CHANGE END] Finish amend agent priority with value:{$value} with address: {$inetAddress} on stream: {$streamName}, partition: {$partition}")
  }

  /**
    * Updates one of options
    *
    * @param partition
    * @param f
    */
  private def updateMySettings(partition: Int, f: (AgentConfiguration) => Unit) = this.synchronized {
    val mySettings = dlm.get[AgentConfiguration](getMyPath(partition))
    mySettings.foreach(s => {
      f(s)
      //println(s"ConfUpdate: $s")
      dlm.setData(getMyPath(partition), s)
    })
  }

  /**
    * Returns participants of the stream for certain partition
    *
    * @param partition
    * @return
    */
  def getStreamPartitionParticipants(partition: Int): List[String] = this.synchronized {
    val agentsOpt = dlm.getAllSubPath(getPartitionPath(partition))
    if (agentsOpt.isDefined)
      agentsOpt.get
    else
      Nil
  }

  def getStreamPartitionParticipantsData(partition: Int): List[AgentConfiguration] = this.synchronized {
    val agentsDataOpt = dlm.getAllSubNodesData[AgentConfiguration](getPartitionPath(partition))
    if (agentsDataOpt.isDefined)
      agentsDataOpt.get
    else
      Nil
  }

  def getBestMasterCandidate(partition: Int): MasterConfiguration = this.synchronized {
    val agentsData = getStreamPartitionParticipantsData(partition)
    //agentsData.foreach(d => println(s"Candidate: $d"))
    val agents = agentsData.sortBy(x => -(x.weight + x.penalty))
    val bestCandidate = MasterConfiguration(agents.last.agentAddress, agents.last.uniqueAgentID)
    bestCandidate
  }


  def demoteMeAsMaster(partition: Int, isUpdatePriority: Boolean = true) = this.synchronized {
    //try to remove old master
    val master = getCurrentMaster(partition)
    master foreach {
      masterSettings =>
        if (masterSettings.agentAddress == inetAddress && masterSettings.uniqueAgentId == agentID) {
          PeerAgent.logger.info(s"[MASTER DELETE BEGIN] Delete agent as MASTER on address: {$inetAddress} from stream: {$streamName}, partition:{$partition}.")
          dlm.delete(getPartitionMasterPath(partition))
          PeerAgent.logger.info(s"[MASTER DELETE END] Agent ($inetAddress) - I'm no longer the master for stream/partition: ($streamName,$partition).")

          if (isUpdatePriority)
            partitions foreach { p => updateMyPriority(p, value = -1) }

          masterMap -= partition
        }
    }
  }

  /**
    * removes stalled artifacts
    */
  private def removeLastSessionArtifacts() = {
    partitions foreach { p => removeLastSessionArtifact(p) }
  }

  /**
    * removes stalled artifacts
    *
    * @param partition
    */
  private def removeLastSessionArtifact(partition: Int) = this.synchronized {
    val agents = getStreamPartitionParticipants(partition)
    agents foreach { agent =>
      if (agent.contains("agent_" + inetAddress + "_") && !agent.contains(agentID.toString)) {
        if (PeerAgent.logger.isDebugEnabled) {
          PeerAgent.logger.debug(s"[INIT CLEAN] Delete agent on address:{$agent} from stream:{$streamName}, partition:{$partition}.")
        }
        try {
          dlm.delete(getMyPath(partition))
        } catch {
          case e: KeeperException =>
        }
      }
    }

    demoteMeAsMaster(partition)

    if (PeerAgent.logger.isDebugEnabled) {
      PeerAgent.logger.debug(s"[INIT CLEAN FINISHED] Delete agent on address:{$inetAddress} from stream: {$streamName}, partition: {$partition}")
    }
  }

  /**
    * does actual master assignment
    *
    * @param partition
    */
  def assignMeAsMaster(partition: Int) = this.synchronized {
    assert(!dlm.exist(getPartitionMasterPath(partition)))
    val masterConf = MasterConfiguration(inetAddress, agentID)
    dlm.create[MasterConfiguration](
      getPartitionMasterPath(partition),
      masterConf,
      CreateMode.EPHEMERAL)
    partitions foreach { p => updateMyPriority(p, value = +1) }
    masterMap += (partition -> masterConf)
    PeerAgent.logger.info(s"[SET MASTER ANNOUNCE] ($inetAddress) - I was elected as master for stream/partition: ($streamName,$partition).")
  }

  def doLocked[T](f: => T) = this.synchronized {
    LockUtil.withZkLockOrDieDo[T](dlm.getLock(getStreamLockPath()), (100, TimeUnit.SECONDS), Some(PeerAgent.logger), f)
  }

  def setSubscriberStateWatcher(partition: Int, watcher: Watcher) =
      dlm.setWatcher(getSubscribersEventPath(partition), watcher)

  def getPartitionSubscribers(partition: Int) = {
    val subscribersPathOpt = dlm.getAllSubNodesData[String](getSubscribersDataPath(partition))
    val s = Set[String]().empty
    s ++ {
      if (subscribersPathOpt.isDefined) subscribersPathOpt.get else Nil
    }
  }

  def getStream() = streamName

  def getPartitionPath(partition: Int) = s"/producers/agents/$streamName/$partition"

  def getMyPath(partition: Int) = s"${getPartitionPath(partition)}/agent_${inetAddress}_$agentID"

  def getPartitionLockPath(partition: Int) = s"/producers/lock_master/$streamName/$partition"

  def getPartitionMasterPath(partition: Int) = s"/producers/master/$streamName/$partition"

  def getLockVotingPath(partition: Int) = s"/producers/lock_voting/$streamName/$partition"

  def getSubscribersEventPath(partition: Int) = s"/subscribers/event/$streamName/$partition"

  def getSubscribersDataPath(partition: Int) = s"/subscribers/agents/$streamName/$partition"

  def getStreamLockPath() = s"/global/stream/$streamName"

  def dumpPartitionsOwnership() = this.synchronized {
    val partitionsWhereMeIsMaster = masterMap.filter(kv => kv._2.agentAddress == inetAddress).keys
    PeerAgent.logger.info(s"I am $inetAddress with ID $agentID and is master for $partitionsWhereMeIsMaster")
  }
}
