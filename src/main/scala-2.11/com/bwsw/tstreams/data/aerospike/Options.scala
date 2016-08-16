package com.bwsw.tstreams.data.aerospike

import com.aerospike.client.Host
import com.aerospike.client.policy.{ClientPolicy, Policy, WritePolicy}

/**
  * @param namespace    Aerospike namespace
  * @param hosts        Aerospike hosts to connect
  * @param clientPolicy custom client policy for storage
  * @param writePolicy  custom write policy for storage
  * @param readPolicy   custom read policy for storage
  */
class Options(val namespace: String,
              val hosts: Set[Host],
              var clientPolicy: ClientPolicy = null,
              var writePolicy: WritePolicy = null,
              var readPolicy: Policy = null) {

  if (namespace == null)
    throw new Exception("Namespace can't be null")

  /**
    * Client policy
    */
  if (clientPolicy == null)
    clientPolicy = new ClientPolicy()

  /**
    * Write policy
    */
  if (writePolicy == null)
    writePolicy = new WritePolicy()

  /**
    * Read policy
    */
  if (readPolicy == null)
    readPolicy = new Policy()
}
