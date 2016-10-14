package com.bwsw.tstreams.common

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 14.10.16.
  */
class ResourceSharedBackingSourceMap[K, V, T](resourceMap: ResourceCountingMap[K, V, T]) {
  private val map = mutable.Map[K, V]()

  def get(key: K): Option[V] = map.synchronized {
    map.get(key)
  }

  def put(key: K, value: => V): Unit = map.synchronized {
    val valueOpt = resourceMap.acquire(key)
    if(valueOpt.isDefined) {
      map.put(key, valueOpt.get)
    } else {
      val res = value
      resourceMap.place(key, res)
      put(key, res)
    }
  }

  def remove(key: K) = map.synchronized {
    map.get(key).foreach(_ => {
      map.remove(key)
      resourceMap.release(key)
    })
  }

  def forceRemove(key: K) = map.synchronized {
    map.get(key).foreach(_ => {
      map.remove(key)
      resourceMap.forceRelease(key)
    })
  }

  def clear() = map.synchronized {
    map.keys.foreach(key => remove(key))
  }
}
