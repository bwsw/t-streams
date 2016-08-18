package com.bwsw.tstreams.coordination.producer.p2p

import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.common.{LockUtil, ZookeeperDLMService}
import org.apache.zookeeper.{KeeperException, CreateMode}

import scala.collection.mutable

/**
  * Created by ivan on 17.08.16.
  */
class PartitionMasterManager(dlm: ZookeeperDLMService, myIPAddress: String, streamName: String, partitions: Set[Int]) {

  private val masterMap = mutable.Map[Int, String]()
  var agentID: Int = 0

  def getPartitionMasterLocally(partition: Int, default: String): String = {
    val opt = masterMap get partition
    if(opt.isDefined)
      opt.get
    else
      default
  }

  def putPartitionMasterLocally(partition: Int, agent: String) = {
    masterMap += (partition -> agent)
  }

  def removePartitionMasterLocally(partition: Int) = {
    masterMap -= partition
  }


  /**
    * Return master for concrete partition
    *
    * @param partition Partition to set
    * @return Master address
    */
  def getCurrentMaster(partition: Int): Option[MasterSettings] = this.synchronized {
      LockUtil.withZkLockOrDieDo[Option[MasterSettings]](dlm.getLock(getPartitionLockPath(partition)), (100, TimeUnit.SECONDS), Some(PeerAgent.logger), () => {
        val masterOpt = dlm.get[MasterSettings](getPartitionMasterPath(partition))
        if (PeerAgent.logger.isDebugEnabled)
        {
          PeerAgent.logger.debug(s"[REQUEST CURRENT MASTER] Agent: ${masterOpt.getOrElse("None")} is current master on stream: {$streamName}, partition: {$partition}.")
        }
        masterOpt
      })
  }

  /**
    * Does bootstrap for agent
    *
    * @param isLowPriorityToBeMaster
    * @param uniqueAgentId
    */
  def bootstrap(isLowPriorityToBeMaster: Boolean, uniqueAgentId: Int) = this.synchronized {
    agentID = uniqueAgentId

    partitions foreach { p =>

      // save initial records to zk
      val penalty = if (isLowPriorityToBeMaster) PeerAgent.LOW_PRIORITY_PENALTY else 0

      val settings = AgentSettings(myIPAddress, priority = 0, penalty)
      dlm.create[AgentSettings](getMyPath(p), settings, CreateMode.EPHEMERAL)
    }

    if (PeerAgent.logger.isDebugEnabled)
    {
      PeerAgent.logger.debug(s"[INIT] End initialize agent with address:{$myIPAddress}, " +
        s"stream: {$streamName}, partitions: {${partitions.mkString(",")}")
    }

    removeLastSessionArtifacts()

  }

  /**
    * Amend agent priority
    *
    * @param partition Partition to update priority
    * @param value     Value which will be added to current priority
    */
  private def updateMyPriority(partition: Int, value: Int) = {
    if (PeerAgent.logger.isDebugEnabled)
    {
      PeerAgent.logger.debug(s"[PRIOR] Start amend agent priority with value:{$value} with address: {$myIPAddress} on stream: {$streamName}, partition: {$partition}")
    }
    updateMySettings(partition, (s: AgentSettings) => s.priority += value)
    if (PeerAgent.logger.isDebugEnabled)
    {
      PeerAgent.logger.debug(s"[PRIOR] Finish amend agent priority with value:{$value} with address: {$myIPAddress} on stream: {$streamName}, partition: {$partition}")
    }
  }

  /**
    * Updates one of options
    *
    * @param partition
    * @param f
    */
  private def updateMySettings(partition: Int, f: (AgentSettings) => Unit) = this.synchronized {
    val mySettings = dlm.get[AgentSettings](getMyPath(partition)).get
    f(mySettings)
    dlm.setData(getMyPath(partition), mySettings)
  }

  /**
    * Returns participants of the stream for certain partition
    *
    * @param partition
    * @return
    */
  def getStreamPartitionParticipants(partition: Int): List[String] = this.synchronized {
    val agentsOpt = dlm.getAllSubPath(getPartitionPath(partition))
    if(agentsOpt.isDefined)
      agentsOpt.get
    else
      Nil
  }

  def getStreamPartitionParticipantsData(partition: Int): List[AgentSettings] = this.synchronized {
    val agentsDataOpt = dlm.getAllSubNodesData[AgentSettings](getPartitionPath(partition))
    if(agentsDataOpt.isDefined)
      agentsDataOpt.get
    else
      Nil
  }

  def getBestMasterCandidate(partition: Int): String = this.synchronized {
    val agentsData = getStreamPartitionParticipantsData(partition)
    val agents = agentsData.sortBy(x => x.priority - x.penalty)
    val bestCandidate = agents.last.agentAddress
    bestCandidate
  }

  /**
    * Unset this agent as master on concrete partition
    *
    * @param partition Partition to set
    */
  private def demoteMaster(partition: Int) = this.synchronized {
    LockUtil.withZkLockOrDieDo[Unit](dlm.getLock(getPartitionLockPath(partition)), (100, TimeUnit.SECONDS), Some(PeerAgent.logger), () => {
      dlm.delete(getPartitionMasterPath(partition))
      if (PeerAgent.logger.isDebugEnabled) {
        PeerAgent.logger.debug(s"[USET MASTER ANNOUNCE] Agent ($myIPAddress) - I'm no longer the master for stream/partition: ($streamName,$partition).")
      }
    })
  }

  def demoteMeAsMaster(partition: Int) = this.synchronized {
    //try to remove old master
    val master = getCurrentMaster(partition)
    master foreach {
      masterSettings =>
        if (masterSettings.agentAddress == myIPAddress && masterSettings.uniqueAgentId != agentID) {
          if (PeerAgent.logger.isDebugEnabled)
          {
            PeerAgent.logger.debug(s"[INIT CLEAN] Delete agent as MASTER on address: {$myIPAddress} from stream: {$streamName}, partition:{$partition} because id was overdue.")
          }
          demoteMaster(partition)
          partitions foreach { p => updateMyPriority(partition, value = 1) }
          masterMap -= partition
      }
    }
  }

  /**
    * removes stalled artifacts
    */
  private def removeLastSessionArtifacts() = {
    partitions foreach { p => removeLastSessionArtifact(p)}
  }

  /**
    * removes stalled artifacts
    *
    * @param partition
    */
  private def removeLastSessionArtifact(partition: Int) = this.synchronized {
    val agents = getStreamPartitionParticipants(partition)
    agents foreach { agent =>
      if(agent.contains("agent_" + myIPAddress + "_") && !agent.contains(agentID.toString)) {
        if (PeerAgent.logger.isDebugEnabled)
        {
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

    if (PeerAgent.logger.isDebugEnabled)
    {
      PeerAgent.logger.debug(s"[INIT CLEAN FINISHED] Delete agent on address:{$myIPAddress} from stream: {$streamName}, partition: {$partition}")
    }
  }

  def assignMeAsMaster(partition: Int) = {
      LockUtil.withZkLockOrDieDo[Unit](dlm.getLock(getPartitionLockPath(partition)), (100, TimeUnit.SECONDS), Some(PeerAgent.logger), () => {
        assert(!dlm.exist(getPartitionMasterPath(partition)))
        dlm.create[MasterSettings](
          getPartitionMasterPath(partition),
          MasterSettings(myIPAddress, agentID),
          CreateMode.EPHEMERAL)
        partitions foreach { p => updateMyPriority(partition, value = -1) }
        masterMap += (partition -> myIPAddress)
        PeerAgent.logger.info(s"[SET MASTER ANNOUNCE] ($myIPAddress) - I was elected as master for stream/partition: ($streamName,$partition).")
      })
  }

  def withElectionLockDo(partition: Int, f: () => String): String =
    LockUtil.withZkLockOrDieDo[String](dlm.getLock(getLockVotingPath(partition)), (100, TimeUnit.SECONDS), Some(PeerAgent.logger), f)


      private def getPartitionPath(partition: Int)        = s"/producers/agents/$streamName/$partition"
  private def getMyPath(partition: Int)               = s"${getPartitionPath(partition)}/agent_${myIPAddress}_$agentID"
  private def getPartitionLockPath(partition: Int)    = s"/producers/lock_master/$streamName/$partition"
  private def getPartitionMasterPath(partition: Int)  = s"/producers/master/$streamName/$partition"
  private def getLockVotingPath(partition: Int)       = s"/producers/lock_voting/$streamName/$partition"
}
