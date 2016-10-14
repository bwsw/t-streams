package com.bwsw.tstreams.common

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 14.10.16.
  */
class ResourceSharedBackingSourceMap[K, V, T](resourceMap: ResourceCountingMap[K, V, T]) {
  private val map = mutable.Map[K, V]()

  def get(key: K): Option[V] = map.synchronized {
    val valueOpt = map.get(key)
    if(valueOpt.isEmpty) {
      val valueOpt = resourceMap.acquire(key)
      valueOpt.foreach(v => map.put(key, valueOpt.get))
    }
    map.get(key)
  }

  def putIfNotExists(key: K, value: => V): Boolean = map.synchronized {
    remove(key)
    val resourceMapValueOpt = resourceMap.acquire(key)
    resourceMap.release(key)
    if(resourceMapValueOpt.isEmpty) {
      resourceMap.place(key, value)
      map.put(key, resourceMap.acquire(key).get)
      true
    } else
      false
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
