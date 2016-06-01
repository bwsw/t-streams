package com.bwsw.tstreams.agents.consumer

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.consumer.Offsets.IOffset
import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.policy.AbstractPolicy
import com.bwsw.tstreams.generator.IUUIDGenerator

/**
 * Basic consumer options
 * @param converter User defined or predefined converter which convert storage type in to usertype
 * @param offset Offset from which start to read
 * @param useLastOffset Use or not last offset for specific consumer
 *                      if last offset not exist, offset will be used
 * @param transactionsPreload Buffer size of preloaded transactions
 * @param dataPreload Buffer size of preloaded data for each consumed transaction
 * @param readPolicy Strategy how to read from concrete stream
 * @param consumerKeepAliveInterval Concrete consumer update interval
 * @param txnGenerator Generator for generating UUIDs
 * @tparam DATATYPE Storage type
 * @tparam USERTYPE User type
 */
class BasicConsumerOptions[DATATYPE,USERTYPE](val transactionsPreload : Int,
                                              val dataPreload : Int,
                                              val consumerKeepAliveInterval : Int,
                                              val converter : IConverter[DATATYPE,USERTYPE],
                                              val readPolicy : AbstractPolicy,
                                              val consumerCoordinatorSettings: ConsumerCoordinationSettings,
                                              val offset : IOffset,
                                              val txnGenerator: IUUIDGenerator,
                                              val useLastOffset : Boolean = true) {
  if (transactionsPreload < 1)
    throw new IllegalArgumentException("incorrect transactionPreload value, should be greater or equal one")

  if (dataPreload < 1)
    throw new IllegalArgumentException("incorrect transactionDataPreload value, should be greater or equal one")

  if (consumerKeepAliveInterval < 1)
    throw new IllegalArgumentException("incorrect consumerKeepAliveInterval value, should be greater or equal one")
}

//TODO asserts
class ConsumerCoordinationSettings(val agentAddress : String,
                                   val prefix : String,
                                   val zkHosts : List[InetSocketAddress],
                                   val zkSessionTimeout : Int,
                                   val threadPoolAmount: Int = -1)