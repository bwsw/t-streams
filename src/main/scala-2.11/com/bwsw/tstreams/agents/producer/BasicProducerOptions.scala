package com.bwsw.tstreams.agents.producer

import java.net.InetSocketAddress
import com.bwsw.tstreams.agents.producer.InsertionType.InsertType
import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.coordination.transactions.transport.traits.ITransport
import com.bwsw.tstreams.policy.AbstractPolicy
import com.bwsw.tstreams.generator.IUUIDGenerator
import scala.language.existentials

/**
 * Class for Basic Producer Options
 * @param transactionTTL Single transaction live time
 * @param transactionKeepAliveInterval Update transaction interval which is used to keep alive transaction in time when it is opened
 * @param producerKeepAliveInterval Update producer interval for updating producer info
 * @param writePolicy Strategy for selecting next partition
 * @param converter User defined or basic converter for converting USERTYPE objects to DATATYPE objects(storage object type)
 * @param insertType Insertion Type (only BatchInsert and SingleElementInsert are allowed now)
 * @param txnGenerator Generator for generating UUIDs
 * @tparam USERTYPE User object type
 * @tparam DATATYPE Storage object type
 */
class BasicProducerOptions[USERTYPE,DATATYPE](val transactionTTL : Int,
                                              val transactionKeepAliveInterval : Int,
                                              val producerKeepAliveInterval : Int,
                                              val writePolicy : AbstractPolicy,
                                              val insertType: InsertType,
                                              val txnGenerator: IUUIDGenerator,
                                              val producerCoordinationSettings : ProducerCoordinationOptions,
                                              val converter : IConverter[USERTYPE,DATATYPE]) {

  /**
   * Transaction minimum ttl time
   */
    private val minTxnTTL = 3

  /**
   * Options validating
   */
    if (transactionTTL < minTxnTTL)
      throw new IllegalArgumentException(s"transactionTTL should be greater or equal than $minTxnTTL")

    if (transactionKeepAliveInterval < 1)
      throw new IllegalArgumentException(s"transactionKeepAliveInterval should be greater or equal than 1")

    if (transactionKeepAliveInterval.toDouble > transactionTTL.toDouble / 3.0)
      throw new IllegalArgumentException("transactionTTL should be three times greater than transaction")

    if (producerKeepAliveInterval < 1)
      throw new IllegalArgumentException("producerKeepAlive interval should be greater or equal than 1")


    insertType match {
      case InsertionType.SingleElementInsert =>

      case InsertionType.BatchInsert(size) =>
        if (size <= 0)
          throw new IllegalArgumentException("batch size must be greater or equal 1")

      case _ =>
        throw new IllegalArgumentException("Insert type can't be resolved")
    }
}


class ProducerCoordinationOptions(val agentAddress : String,
                                   val zkHosts : List[InetSocketAddress],
                                   val zkRootPath : String,
                                   val zkTimeout : Int,
                                   val isLowPriorityToBeMaster : Boolean,
                                   val transport: ITransport,
                                   val transportTimeout : Int,
                                   val threadPoolAmount: Int = -1)