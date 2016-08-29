package com.bwsw.tstreams.agents.consumer.subscriber

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.consumer
import com.bwsw.tstreams.agents.consumer.Offset.IOffset
import com.bwsw.tstreams.agents.consumer.subscriber.QueueBuilder.InMemory
import com.bwsw.tstreams.common.AbstractPolicy
import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.generator.IUUIDGenerator

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
/**
  * Class which represents options for subscriber consumer V2
  *
  * @param transactionsPreload
  * @param dataPreload
  * @param converter
  * @param readPolicy
  * @param offset
  * @param txnGenerator
  * @param agentAddress
  * @param zkRootPath
  * @param zkHosts
  * @param zkSessionTimeout
  * @param zkConnectionTimeout
  * @param txnBufferWorkersThreadPoolAmount
  * @param useLastOffset
  * @tparam T
  */
case class Options[T](val transactionsPreload:    Int,
                      val dataPreload:            Int,
                      val converter:              IConverter[Array[Byte], T],
                      val readPolicy:             AbstractPolicy,
                      val offset:                 IOffset,
                      val txnGenerator:           IUUIDGenerator,
                      val useLastOffset:          Boolean,
                      val agentAddress:           String,
                      val zkRootPath:             String,
                      val zkHosts:                Set[InetSocketAddress],
                      val zkSessionTimeout:       Int,
                      val zkConnectionTimeout:    Int,
                      val txnBufferWorkersThreadPoolAmount:       Int      = 1,
                      val processingEngineWorkersThreadAmount:    Int      = 1,
                      val pollingFrequencyDelay:                  Int      = 1000,
                      val txnQueueBuilder:                        QueueBuilder.Abstract  = new InMemory
                      ) {

  def getConsumerOptions() = consumer.Options[T](
                                transactionsPreload = transactionsPreload,
                                dataPreload = dataPreload,
                                converter = converter,
                                readPolicy = readPolicy,
                                offset = offset,
                                txnGenerator = txnGenerator,
                                useLastOffset = useLastOffset)

}

