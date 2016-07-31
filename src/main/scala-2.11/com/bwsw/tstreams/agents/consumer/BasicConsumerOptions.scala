package com.bwsw.tstreams.agents.consumer

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.consumer.Offsets.IOffset
import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.generator.IUUIDGenerator
import com.bwsw.tstreams.policy.AbstractPolicy

/**
  * @param converter                 User defined or predefined converter which convert storage type into usertype
  * @param offset                    Offset from which start to read
  * @param useLastOffset             Use or not last offset for specific consumer
  *                                  if last offset not exist, offset will be used
  * @param transactionsPreload       Buffer size of preloaded transactions
  * @param dataPreload               Buffer size of preloaded data for each consumed transaction
  * @param readPolicy                Strategy how to read from concrete stream
  * @param txnGenerator              Generator for generating UUIDs
  * @tparam USERTYPE User type
  */
class BasicConsumerOptions[USERTYPE](val transactionsPreload: Int, val dataPreload: Int, val converter: IConverter[Array[Byte], USERTYPE], val readPolicy: AbstractPolicy, val offset: IOffset, val txnGenerator: IUUIDGenerator, val useLastOffset: Boolean = true) {
  if (transactionsPreload < 1)
    throw new IllegalArgumentException("incorrect transactionPreload value, should be greater or equal one")

  if (dataPreload < 1)
    throw new IllegalArgumentException("incorrect transactionDataPreload value, should be greater or equal one")

}

/**
  *
  * @param agentAddress     Subscriber address in network
  * @param zkRootPath       Zk root prefix
  * @param zkHosts          Zk hosts to connect
  * @param zkSessionTimeout Zk session timeout
  * @param threadPoolAmount Thread pool amount which is used by
  *                         [[com.bwsw.tstreams.agents.consumer.subscriber.BasicSubscribingConsumer]]]
  *                         by default (threads_amount == used_consumer_partitions)
  */
//TODO validate params
class SubscriberCoordinationOptions(val agentAddress: String,
                                    val zkRootPath: String,
                                    val zkHosts: List[InetSocketAddress],
                                    val zkSessionTimeout: Int,
                                    val zkConnectionTimeout: Int,
                                    val threadPoolAmount: Int = -1)