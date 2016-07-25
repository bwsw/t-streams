package com.bwsw.tstreams.data.aerospike

import java.util.concurrent.locks.ReentrantLock

import com.aerospike.client.policy.ClientPolicy
import com.aerospike.client.{Host, AerospikeClient}


/**
 * Factory for creating Aerospike storage instances
 */
class AerospikeStorageFactory{

  /**
   * Map for memorize clients which are already created
   */
  private val aerospikeClients = scala.collection.mutable.Map[(List[Host], ClientPolicy), AerospikeClient]()

  private var isClosed = false

  /**
   * Lock for providing getInstance thread safeness
   */
  private val lock = new ReentrantLock(true)

  /**
   * @param aerospikeOptions Options of aerospike client
   * @return Instance of CassandraStorage
   */
  def getInstance(aerospikeOptions: AerospikeStorageOptions) : AerospikeStorage = {
    lock.lock()

    if(isClosed)
      throw new IllegalStateException("AerospikeStorageFactory is closed. This is the illegal usage of the object.")


    val client = {
      if (aerospikeClients.contains((aerospikeOptions.hosts, aerospikeOptions.clientPolicy))) {
        aerospikeClients((aerospikeOptions.hosts, aerospikeOptions.clientPolicy))
      }
      else{
        val client = new AerospikeClient(aerospikeOptions.clientPolicy,aerospikeOptions.hosts:_*)
        aerospikeClients((aerospikeOptions.hosts, aerospikeOptions.clientPolicy)) = client
        client
      }
    }

    val inst = new AerospikeStorage(client, aerospikeOptions)
    lock.unlock()

    inst
  }

  /**
   * Close all factory storage instances
   */
  def closeFactory() : Unit = {
    lock.lock()

    if(isClosed)
      throw new IllegalStateException("AerospikeStorageFactory is closed. This is repeatable close operation.")
    isClosed = true

    aerospikeClients.foreach(x=>x._2.close())
    aerospikeClients.clear()
    lock.unlock()
  }
}
