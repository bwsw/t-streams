package com.bwsw.tstreams.velocity

import java.net.InetSocketAddress
import java.util.UUID

import com.datastax.driver.core.Cluster

import scala.collection.mutable.ListBuffer

object Validator {
  def isSorted(list: ListBuffer[UUID]): Boolean = {
    if (list.isEmpty)
      return true
    var curVal = list.head
    var v = 0
    list foreach { el =>
      if (el.timestamp() < curVal.timestamp()) {
        println(s"value=$v")
        return false
      }
      if (el.timestamp() > curVal.timestamp())
        curVal = el
      v += 1
    }
    true
  }

  def main(args: Array[String]) {
    //    if (args.length != 1)
    //      throw new IllegalArgumentException("specify [keyspace]")
    //    val keyspace = args(0)

    val cluster = Cluster.builder().addContactPointsWithPorts(new InetSocketAddress("176.120.27.82", 9042)).build()
    val session = cluster.connect()

    val set = session.execute(s"select * from velocity.commit_log").all()
    val it = set.iterator()
    val buffers = scala.collection.mutable.Map[Int, ListBuffer[UUID]]()

    while (it.hasNext) {
      val row = it.next()
      val partition = row.getInt("partition")
      val uuid = row.getUUID("transaction")
      if (!buffers.contains(partition)) {
        buffers(partition) = ListBuffer(uuid)
      } else {
        buffers(partition) += uuid
      }
    }

    val checkVal = buffers.map(x => isSorted(x._2)).reduceLeft((a, b) => a & b)

    if (checkVal)
      println("sorted")
    else
      println("not sorted")

    cluster.close()
    session.close()
  }
}
