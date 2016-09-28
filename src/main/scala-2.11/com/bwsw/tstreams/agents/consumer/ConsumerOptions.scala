package com.bwsw.tstreams.agents.consumer

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.consumer.Offset.IOffset
import com.bwsw.tstreams.common.AbstractPolicy
import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.generator.ITransactionGenerator

/**
  * @param converter            User defined or predefined converter which convert storage type into user type
  * @param offset               Offset from which start to read
  * @param useLastOffset        Use or not last offset for specific consumer
  *                             if last offset not exist, offset will be used
  * @param transactionsPreload  Buffer size of preloaded transactions
  * @param dataPreload          Buffer size of preloaded data for each consumed transaction
  * @param readPolicy           Strategy how to read from concrete stream
  * @param transactionGenerator Generator for generating IDs
  * @tparam T User type
  */
case class ConsumerOptions[T](transactionsPreload: Int,
                              dataPreload: Int,
                              converter: IConverter[Array[Byte], T],
                              readPolicy: AbstractPolicy,
                              offset: IOffset,
                              transactionGenerator: ITransactionGenerator,
                              useLastOffset: Boolean = true) {
  if (transactionsPreload < 1)
    throw new IllegalArgumentException("Incorrect transactionPreload value, should be greater than or equal to one.")

  if (dataPreload < 1)
    throw new IllegalArgumentException("Incorrect transactionDataPreload value, should be greater than or equal to one.")

}

/**
  *
  * @param agentAddress     Subscriber address in network
  * @param zkRootPath       Zk root prefix
  * @param zkHosts          Zk hosts to connect
  * @param zkSessionTimeout Zk session timeout
  * @param threadPoolAmount Thread pool amount which is used by
  *                         [[com.bwsw.tstreams.agents.consumer.subscriber.Subscriber]]]
  *                         by default (threads_amount == used_consumer_partitions)
  */
class SubscriberCoordinationOptions(val agentAddress: String,
                                    val zkRootPath: String,
                                    val zkHosts: List[InetSocketAddress],
                                    val zkSessionTimeout: Int,
                                    val zkConnectionTimeout: Int,
                                    val threadPoolAmount: Int = -1)


