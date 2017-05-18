package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.agents.consumer
import com.bwsw.tstreams.agents.consumer.Offset.IOffset
import com.bwsw.tstreams.agents.consumer.subscriber.QueueBuilder.InMemory
import com.bwsw.tstreams.common.AbstractPolicy
import com.bwsw.tstreams.generator.ITransactionGenerator

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
/**
  * Class which represents options for subscriber consumer V2
  *
  * @param transactionsPreload
  * @param dataPreload
  * @param readPolicy
  * @param offset
  * @param transactionGenerator
  * @param agentAddress
  * @param zkRootPath
  * @param zkHosts
  * @param zkSessionTimeoutMs
  * @param zkConnectionTimeoutMs
  * @param transactionBufferWorkersThreadPoolAmount
  * @param useLastOffset
  */
case class SubscriberOptions(val transactionsPreload: Int,
                             val dataPreload: Int,
                             val readPolicy: AbstractPolicy,
                             val offset: IOffset,
                             val transactionGenerator: ITransactionGenerator,
                             val useLastOffset: Boolean,
                             val rememberFirstStartOffset: Boolean = true,
                             val agentAddress: String,
                             val zkRootPath: String,
                             val zkHosts: String,
                             val zkSessionTimeoutMs: Int,
                             val zkConnectionTimeoutMs: Int,
                             val zkRetryDelayMs: Int,
                             val zkRetryCount: Int,
                             val transactionBufferWorkersThreadPoolAmount: Int = 1,
                             val processingEngineWorkersThreadAmount: Int = 1,
                             val pollingFrequencyDelayMs: Int = 1000,
                             val transactionQueueMaxLengthThreshold: Int = 10000,
                             val transactionsQueueBuilder: QueueBuilder.Abstract = new InMemory
                            ) {

  def getConsumerOptions() = consumer.ConsumerOptions(
    transactionsPreload = transactionsPreload,
    dataPreload = dataPreload,
    readPolicy = readPolicy,
    offset = offset,
    transactionGenerator = transactionGenerator,
    checkpointAtStart = rememberFirstStartOffset,
    useLastOffset = useLastOffset)

}

