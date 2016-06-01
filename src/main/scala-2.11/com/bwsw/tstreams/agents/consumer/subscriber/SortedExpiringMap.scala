package com.bwsw.tstreams.agents.consumer.subscriber

import java.util
import java.util.Comparator
import org.apache.commons.collections4.map.PassiveExpiringMap
import org.apache.commons.collections4.map.PassiveExpiringMap.ExpirationPolicy

/**
 * Map with expiring records based on [[expirationPolicy]]]
 * in order which is determined by [[comparator]]]
 * @param comparator Comparator to organize order
 * @param expirationPolicy Policy of record expiration
 * @tparam K type
 * @tparam V type
 */
class SortedExpiringMap[K,V] (comparator : Comparator[K], expirationPolicy: ExpirationPolicy[K,V]) {

  private val treeMap = new util.TreeMap[K, V](comparator)

  private val map = new PassiveExpiringMap[K, V](expirationPolicy, treeMap)

  def put(key : K, value : V) : Unit = map.put(key,value)

  def get(key : K): V = map.get(key)

  def exist(key : K) : Boolean = map.containsKey(key)

  def remove(key : K) : Unit = map.remove(key)

  def entrySetIterator() = map.entrySet().iterator()
}
