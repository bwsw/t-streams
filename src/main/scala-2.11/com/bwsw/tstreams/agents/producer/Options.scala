package com.bwsw.tstreams.agents.producer

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.producer.DataInsertType.AbstractInsertType
import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.coordination.producer.p2p.PeerAgent
import com.bwsw.tstreams.coordination.producer.transport.traits.ITransport
import com.bwsw.tstreams.generator.IUUIDGenerator
import com.bwsw.tstreams.policy.AbstractPolicy

import scala.language.existentials

/**
  * @param transactionTTL               Single transaction live time
  * @param transactionKeepAliveInterval Update transaction interval which is used to keep alive transaction in time when it is opened
  * @param writePolicy                  Strategy for selecting next partition
  * @param converter                    User defined or basic converter for converting USERTYPE objects to DATATYPE objects(storage object type)
  * @param insertType                   Insertion Type (only BatchInsert and SingleElementInsert are allowed now)
  * @param txnGenerator                 Generator for generating UUIDs
  * @tparam USERTYPE User object type
  */
class Options[USERTYPE](val transactionTTL: Int, val transactionKeepAliveInterval: Int, val writePolicy: AbstractPolicy, val insertType: AbstractInsertType, val txnGenerator: IUUIDGenerator, val coordinationOptions: CoordinationOptions, val converter: IConverter[USERTYPE, Array[Byte]]) {

  /**
    * Transaction minimum ttl time
    */
  private val minTxnTTL = 3

  /**
    * Options validating
    */
  if (transactionTTL < minTxnTTL)
    throw new IllegalArgumentException(s"Option transactionTTL must be greater or equal than $minTxnTTL")

  if (transactionKeepAliveInterval < 1)
    throw new IllegalArgumentException(s"Option transactionKeepAliveInterval must be greater or equal than 1")

  if (transactionKeepAliveInterval.toDouble > transactionTTL.toDouble / 3.0)
    throw new IllegalArgumentException("Option transactionTTL must be at least three times greater or equal than transaction")

  insertType match {
    case DataInsertType.SingleElementInsert =>

    case DataInsertType.BatchInsert(size) =>
      if (size <= 0)
        throw new IllegalArgumentException("Batch size must be greater or equal 1")

    case _ =>
      throw new IllegalArgumentException("Insert type can't be resolved")
  }
}


/**
  * @param agentAddress            Address of producer in network
  * @param zkHosts                 Zk hosts to connect
  * @param zkRootPath              Zk root path for all metadata
  * @param zkSessionTimeout        Zk session timeout
  * @param isLowPriorityToBeMaster Flag which indicate priority to became master on stream/partition
  *                                of this agent
  * @param transport               Transport providing interaction between agents
  * @param transportTimeout        Transport timeout in seconds
  * @param threadPoolAmount        Thread pool amount which is used by
  *                                [[PeerAgent]]]
  *                                by default (threads_amount == used_producer_partitions)
  */
class CoordinationOptions(val agentAddress: String,
                          val zkHosts: List[InetSocketAddress],
                          val zkRootPath: String,
                          val zkSessionTimeout: Int,
                          val zkConnectionTimeout: Int,
                          val isLowPriorityToBeMaster: Boolean,
                          val transport: ITransport,
                          val transportTimeout: Int,
                          val threadPoolAmount: Int = -1)