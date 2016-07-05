package com.bwsw.tstreams.common.zkservice

import java.net.InetSocketAddress

import com.bwsw.tstreams.common.serializer.JsonSerializer
import com.twitter.common.quantity.Amount
import com.twitter.common.zookeeper.{DistributedLockImpl, ZooKeeperClient}
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.{CreateMode, Watcher, ZooKeeper}

import collection.JavaConverters._

/**
 * @param prefix Prefix path for all created entities
 * @param zkHosts Zk hosts to connect
 * @param zkSessionTimeout Zk session timeout to connect
 */
class ZkService(prefix : String, zkHosts : List[InetSocketAddress], zkSessionTimeout : Int){
  //TODO fix zk timeout
  private val sessionTimeout = Amount.of(new Integer(zkSessionTimeout),com.twitter.common.quantity.Time.SECONDS)
  private val hosts = zkHosts.asJava
  private val twitterZkClient: ZooKeeperClient = new ZooKeeperClient(sessionTimeout, hosts)
  //TODO move 7 sec
  private val zkClient = twitterZkClient.get(Amount.of(7L, com.twitter.common.quantity.Time.SECONDS))
  private val serializer = new JsonSerializer
  private val map = scala.collection.mutable.Map[String, DistributedLockImpl]()

  def getLock(path : String) : DistributedLockImpl = {
    if (map.contains(prefix+path))
      map(prefix+path)
    else {
      val lock = new DistributedLockImpl(twitterZkClient, prefix + path)
      map += (prefix+path -> lock)
      lock
    }
  }

  def create[T](path : String, data : T, createMode: CreateMode) = {
    val serialized = serializer.serialize(data)
    var initPath = prefix + path.reverse.dropWhile(_!='/').reverse.dropRight(1)
    if (initPath.isEmpty)
      initPath = "/"
    if (zkClient.exists(initPath, null) == null) {
      val lock = getLock("/producers/create_path_lock")
      lock.lock()
      if (zkClient.exists(initPath, null) == null)
        createPathRecursive(initPath, CreateMode.PERSISTENT)
      lock.unlock()
    }
    if (zkClient.exists(prefix + path, null) == null)
      zkClient.create(prefix+path, serialized.getBytes, Ids.OPEN_ACL_UNSAFE, createMode)
    else {
      throw new IllegalStateException("path already exist")
    }
  }

  def setWatcher(path : String, watcher : Watcher) : Unit = {
    if (zkClient.exists(prefix+path, null) == null) {
      val lock = getLock("/producers/watcher_path_lock")
      lock.lock()
      if (zkClient.exists(prefix+path, null) == null)
        createPathRecursive(prefix+path, CreateMode.PERSISTENT)
      lock.unlock()
    }
    zkClient.getData(prefix + path, watcher, null)
  }

  def notify(path : String) : Unit = {
    if (zkClient.exists(prefix+path, null) != null) {
      zkClient.setData(prefix+path, null, -1)
    }
  }

  def setData(path : String, data : Any) : Unit = {
    val string = serializer.serialize(data)
    zkClient.setData(prefix + path, string.getBytes, -1)
  }

  def exist(path : String) : Boolean = {
    zkClient.exists(prefix + path, null) != null
  }

  def get[T : Manifest](path : String) : Option[T] = {
    if (zkClient.exists(prefix + path, null) == null)
      None
    else {
      val data = zkClient.getData(prefix + path, null, null)
      Some(serializer.deserialize[T](new String(data)))
    }
  }
  
  def getAllSubNodesData[T : Manifest](path : String) : Option[List[T]] = {
    if (zkClient.exists(prefix + path, null) == null)
      None
    else {
      val subNodes = zkClient.getChildren(prefix + path, null).asScala.map(x=>zkClient.getData(prefix+path+"/"+x, null,null))
      val data = subNodes.flatMap(x=> List(serializer.deserialize[T](new String(x)))).toList
      Some(data)
    }
  }

  def getAllSubPath(path : String) : Option[List[String]] = {
    if (zkClient.exists(prefix + path, null) == null)
      None
    else {
      val subNodes = zkClient.getChildren(prefix + path, null).asScala.toList
      Some(subNodes)
    }
  }

  def delete(path : String) = {
    zkClient.delete(prefix+path, -1)
  }

  def deleteRecursive(path : String) : Unit = {
    val children = zkClient.getChildren(prefix+path, null, null).asScala
    if (children.nonEmpty){
      children.foreach{x=>deleteRecursive(path+"/"+x)}
    }

    zkClient.delete(prefix+path,-1)
  }

  private def createPathRecursive(path : String, mode : CreateMode) = {
    val splits = path.split("/").filter(x=>x!="")
    def createRecursive(path : List[String], acc : List[String]) : Unit = path match {
      case Nil =>
        val path = "/" + acc.mkString("/")
        if (zkClient.exists(path,null) == null)
          zkClient.create(path, null, Ids.OPEN_ACL_UNSAFE, mode)

      case h::t =>
        val path = "/" + acc.mkString("/")
        if (zkClient.exists(path,null) == null)
          zkClient.create(path, null, Ids.OPEN_ACL_UNSAFE, mode)
        createRecursive(t, acc :+ h)
    }
    createRecursive(splits.toList.drop(1), List(splits.head))
  }
  
  def isZkConnected =
    zkClient.getState == States.CONNECTED

  def getSessionTimeout =
    zkClient.getSessionTimeout

  def close() = {
    twitterZkClient.close()
    zkClient.close()
  }
}