package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.agents.consumer
import com.bwsw.tstreams.agents.consumer.Offset.IOffset
import com.bwsw.tstreams.agents.consumer.subscriber.QueueBuilder.InMemory
import com.bwsw.tstreams.common.PartitionIterationPolicy

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
  * @param agentAddress
  * @param zkPrefixPath
  * @param transactionBufferWorkersThreadPoolAmount
  * @param useLastOffset
  */
class SubscriberOptions(val transactionsPreload: Int,
                        val dataPreload: Int,
                        val readPolicy: PartitionIterationPolicy,
                        val offset: IOffset,
                        val useLastOffset: Boolean,
                        val rememberFirstStartOffset: Boolean = true,
                        val agentAddress: String,
                        val zkPrefixPath: String,
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
    checkpointAtStart = rememberFirstStartOffset,
    useLastOffset = useLastOffset)

}

