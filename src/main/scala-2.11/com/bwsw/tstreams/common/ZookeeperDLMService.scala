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

  val CREATE_PATH_LOCK = "/locks/create_path_lock"
  val WATCHER_LOCK = "/locks/watcher_path_lock"
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

  ZookeeperDLMService.logger.info("Starting DLM service")

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
    val p = java.nio.file.Paths.get(prefix, path).toString
    if (lockMap.contains(p))
      lockMap(p)
    else {
      val lock = new DistributedLockImpl(twitterZkClient, p)
      lockMap += (p -> lock)
      lock
    }
  }

  /**
    * Creates path recursively with lock
    *
    * @param path
    * @param data
    * @param createMode
    * @tparam T
    * @return
    */
  def create[T](path: String, data: T, createMode: CreateMode) = this.synchronized {
    val serialized = ZookeeperDLMService.serializer.serialize(data)
    val initPath = java.nio.file.Paths.get(prefix,path).toFile.getParentFile().getPath()
    if (zkClient.exists(initPath, null) == null) {
      LockUtil.withZkLockOrDieDo[Unit](getLock(ZookeeperDLMService.CREATE_PATH_LOCK), (100, TimeUnit.SECONDS), Some(ZookeeperDLMService.logger), () => {
        if (zkClient.exists(initPath, null) == null)
          createPathRecursive(initPath, CreateMode.PERSISTENT) })
    }

    val p = java.nio.file.Paths.get(prefix, path).toString
    if (zkClient.exists(p, null) == null)
      zkClient.create(p, serialized.getBytes, Ids.OPEN_ACL_UNSAFE, createMode)
    else {
      throw new IllegalStateException(s"Requested path ${p} already exists.")
    }
  }

  /**
    * Establishes watcher
    *
    * @param path
    * @param watcher
    */
  def setWatcher(path: String, watcher: Watcher): Unit = this.synchronized {
    val p = java.nio.file.Paths.get(prefix, path).toString
    if (zkClient.exists(p, null) == null) {
      LockUtil.withZkLockOrDieDo[Unit](getLock(ZookeeperDLMService.WATCHER_LOCK), (100, TimeUnit.SECONDS), Some(ZookeeperDLMService.logger), () => {
        if (zkClient.exists(p, null) == null)
          createPathRecursive(p, CreateMode.PERSISTENT) })
    }
    zkClient.getData(p, watcher, null)
  }

  /**
    *
    * @param path
    */
  def notify(path: String): Unit = this.synchronized {
    val p = java.nio.file.Paths.get(prefix, path).toString
    if (zkClient.exists(p, null) != null) {
      zkClient.setData(p, null, -1)
    }
  }

  def setData(path: String, data: Any): Unit = this.synchronized {
    val string = ZookeeperDLMService.serializer.serialize(data)
    val p = java.nio.file.Paths.get(prefix, path).toString
    zkClient.setData(p, string.getBytes, -1)
  }

  /**
    * Check if path exists
    *
    * @param path
    * @return
    */
  def exist(path: String): Boolean = this.synchronized {
    val p = java.nio.file.Paths.get(prefix, path).toString
    zkClient.exists(p, null) != null
  }

  /**
    * Get data for specified node
    *
    * @param path
    * @tparam T
    * @return
    */
  def get[T: Manifest](path: String): Option[T] = this.synchronized {
    val p = java.nio.file.Paths.get(prefix, path).toString
    if (zkClient.exists(p, null) == null)
      None
    else {
      val data = zkClient.getData(p, null, null)
      Some(ZookeeperDLMService.serializer.deserialize[T](new String(data)))
    }
  }

  /**
    * Get data of all children nodes
    *
    * @param path
    * @tparam T
    * @return
    */
  def getAllSubNodesData[T: Manifest](path: String): Option[List[T]] = this.synchronized {
    val p = java.nio.file.Paths.get(prefix, path).toString
    if (zkClient.exists(p, null) == null)
      None
    else {
      val subNodes = zkClient.getChildren(p, null)
        .asScala
        .map(x => zkClient.getData(java.nio.file.Paths.get(p, x).toString, null, null))
      val data = subNodes.flatMap(x => List(ZookeeperDLMService.serializer.deserialize[T](new String(x)))).toList
      Some(data)
    }
  }

  /**
    * Get path of all children nodes
    *
    * @param path
    * @return
    */
  def getAllSubPath(path: String): Option[List[String]] = this.synchronized {
    val p = java.nio.file.Paths.get(prefix, path).toString
    if (zkClient.exists(p, null) == null)
      None
    else {
      val subNodes = zkClient.getChildren(p, null).asScala.toList
      Some(subNodes)
    }
  }

  /**
    * Delete path
    *
    * @param path
    */
  def delete(path: String) = this.synchronized {
    val p = java.nio.file.Paths.get(prefix, path).toString
    zkClient.delete(p, -1)
  }

  /**
    * Delete path recursively
    *
    * @param path
    */
  def deleteRecursive(path: String): Unit = this.synchronized {
    val p = java.nio.file.Paths.get(prefix, path).toString
    val children = zkClient.getChildren(p, null, null).asScala
    if (children.nonEmpty) {
      children
        .foreach { x => deleteRecursive(java.nio.file.Paths.get(path, x).toString) }
    }
    zkClient.delete(p, -1)
  }

  /**
    * Create path recursively
    *
    * @param path
    * @param mode
    */
  private def createPathRecursive(path: String, mode: CreateMode): Unit = this.synchronized {
    if (zkClient.exists(path, null) == null) {
      val parent = java.nio.file.Paths.get(path).getParent.toString
      if(parent != "/")
        createPathRecursive(parent, mode)
      zkClient.create(path, null, Ids.OPEN_ACL_UNSAFE, mode)
    }
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
