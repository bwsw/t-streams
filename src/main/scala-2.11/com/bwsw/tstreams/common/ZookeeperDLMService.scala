package com.bwsw.tstreams.common

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import com.twitter.common.quantity.Amount
import com.twitter.common.zookeeper.{DistributedLockImpl, ZooKeeperClient}
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.{CreateMode, Watcher}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * shared objects for ZookeeperDLMService
  */
object ZookeeperDLMService {
  val logger = LoggerFactory.getLogger(this.getClass)
  val serializer = new JsonSerializer
}

//TODO test it harder on zk ephemeral nodes elimination
/**
  * @param prefix           Prefix path for all created entities
  * @param zkHosts          Zk hosts to connect
  * @param zkSessionTimeout Zk session timeout to connect
  */
class ZookeeperDLMService(prefix: String, zkHosts: List[InetSocketAddress], zkSessionTimeout: Int, connectionTimeout: Long) {
  private val st = Amount.of(new Integer(zkSessionTimeout), com.twitter.common.quantity.Time.SECONDS)
  private val ct = Amount.of(connectionTimeout, com.twitter.common.quantity.Time.SECONDS)

  private val lockMap = scala.collection.mutable.Map[String, DistributedLockImpl]()

  private val twitterZkClient = new ZooKeeperClient(st, zkHosts.asJava)
  private val zkClient        = twitterZkClient.get(ct)

  if(ZookeeperDLMService.logger.isDebugEnabled) {
    ZookeeperDLMService.logger.debug("Zookeeper session timeout is set to " + st)
    ZookeeperDLMService.logger.debug("Zookeeper connection timeout is set to " + ct)
  }

  /**
    * receives DLM lock object which can be used later
    *
    * @param path
    * @return
    */
  def getLock(path: String): DistributedLockImpl = this.synchronized {
    if (lockMap.contains(prefix + path))
      lockMap(prefix + path)
    else {
      val lock = new DistributedLockImpl(twitterZkClient, prefix + path)
      lockMap += (prefix + path -> lock)
      lock
    }
  }

  /**
    * Allows to create path in Zookeeper in proper manner (with locks)
    *
    * @param path
    * @param data
    * @param createMode
    * @tparam T
    * @return
    */
  def create[T](path: String, data: T, createMode: CreateMode) = this.synchronized {
    val serialized = ZookeeperDLMService.serializer.serialize(data)
    var initPath = prefix + path.reverse.dropWhile(_ != '/').reverse.dropRight(1)

    if (initPath.isEmpty)
      initPath = "/"

    LockUtil.withZkLockOrDieDo[Unit](getLock(s"/locks/create_path_lock"), (100, TimeUnit.SECONDS), Some(ZookeeperDLMService.logger), () => {
      if (zkClient.exists(initPath, null) == null)
        createPathRecursive(initPath, CreateMode.PERSISTENT) })

    LockUtil.withZkLockOrDieDo[Unit](getLock(s"/locks/create_path_lock"), (100, TimeUnit.SECONDS), Some(ZookeeperDLMService.logger), () => {
      if (zkClient.exists(prefix + path, null) == null)
          zkClient.create(prefix + path, serialized.getBytes, Ids.OPEN_ACL_UNSAFE, createMode)
      else {
        throw new IllegalStateException(s"Path ${prefix + path} requested for creation already exist.")
      }})
  }

  def setWatcher(path: String, watcher: Watcher): Unit = this.synchronized {
    LockUtil.withZkLockOrDieDo[Unit](getLock(s"/locks/watcher_path_lock"), (100, TimeUnit.SECONDS), Some(ZookeeperDLMService.logger), () => {
      if (zkClient.exists(prefix + path, null) == null)
        createPathRecursive(prefix + path, CreateMode.PERSISTENT) })
    zkClient.getData(prefix + path, watcher, null)
  }

  def notify(path: String): Unit = this.synchronized {
    if (zkClient.exists(prefix + path, null) != null) {
      zkClient.setData(prefix + path, null, -1)
    }
  }

  def setData(path: String, data: Any): Unit = this.synchronized {
    val string = ZookeeperDLMService.serializer.serialize(data)
    zkClient.setData(prefix + path, string.getBytes, -1)
  }

  def exist(path: String): Boolean = this.synchronized {
    zkClient.exists(prefix + path, null) != null
  }

  def get[T: Manifest](path: String): Option[T] = this.synchronized {
    if (zkClient.exists(prefix + path, null) == null)
      None
    else {
      val data = zkClient.getData(prefix + path, null, null)
      Some(ZookeeperDLMService.serializer.deserialize[T](new String(data)))
    }
  }

  def getAllSubNodesData[T: Manifest](path: String): Option[List[T]] = this.synchronized {
    if (zkClient.exists(prefix + path, null) == null)
      None
    else {
      val subNodes = zkClient.getChildren(prefix + path, null).asScala.map(x => zkClient.getData(prefix + path + "/" + x, null, null))
      val data = subNodes.flatMap(x => List(ZookeeperDLMService.serializer.deserialize[T](new String(x)))).toList
      Some(data)
    }
  }

  def getAllSubPath(path: String): Option[List[String]] = this.synchronized {
    if (zkClient.exists(prefix + path, null) == null)
      None
    else {
      val subNodes = zkClient.getChildren(prefix + path, null).asScala.toList
      Some(subNodes)
    }
  }

  def delete(path: String) = this.synchronized {
    zkClient.delete(prefix + path, -1)
  }

  def deleteRecursive(path: String): Unit = this.synchronized {
    val children = zkClient.getChildren(prefix + path, null, null).asScala
    if (children.nonEmpty) {
      children.foreach { x => deleteRecursive(path + "/" + x) }
    }

    zkClient.delete(prefix + path, -1)
  }

  private def createPathRecursive(path: String, mode: CreateMode) = this.synchronized {
    val splits = path.split("/").filter(x => x != "")
    def createRecursive(path: List[String], acc: List[String]): Unit = path match {
      case Nil =>
        val path = "/" + acc.mkString("/")
        if (zkClient.exists(path, null) == null)
          zkClient.create(path, null, Ids.OPEN_ACL_UNSAFE, mode)

      case h :: t =>
        val path = "/" + acc.mkString("/")
        if (zkClient.exists(path, null) == null)
          zkClient.create(path, null, Ids.OPEN_ACL_UNSAFE, mode)
        createRecursive(t, acc :+ h)
    }
    createRecursive(splits.toList.drop(1), List(splits.head))
  }

  def isZkConnected = this.synchronized {
    zkClient.getState == States.CONNECTED
  }

  def getSessionTimeout = {
    zkClient.getSessionTimeout
  }

  def close() = this.synchronized {
    twitterZkClient.close()
  }
}
