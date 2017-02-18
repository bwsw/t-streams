package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.agents.consumer
import com.bwsw.tstreams.agents.consumer.subscriber.QueueBuilder.InMemory

object SubscriberOptionsBuilder {
  def fromConsumerOptions(consumerOpts: consumer.ConsumerOptions,
                             agentAddress: String,
                             zkRootPath: String,
                             zkHosts: String,
                             zkSessionTimeout: Int,
                             zkConnectionTimeout: Int,
                             transactionsBufferWorkersThreadPoolAmount: Int = 1,
                             processingEngineWorkersThreadAmount: Int = 1,
                             pollingFrequencyDelay: Int = 1000,
                             transactionsQueueBuilder: QueueBuilder.Abstract = new InMemory): SubscriberOptions =
    new SubscriberOptions(
      transactionsPreload = consumerOpts.transactionsPreload,
      dataPreload = consumerOpts.dataPreload,
      readPolicy = consumerOpts.readPolicy,
      offset = consumerOpts.offset,
      transactionGenerator = consumerOpts.transactionGenerator,
      useLastOffset = consumerOpts.useLastOffset,
      agentAddress = agentAddress,
      zkRootPath = zkRootPath,
      zkHosts = zkHosts,
      zkSessionTimeout = zkSessionTimeout,
      zkConnectionTimeout = zkConnectionTimeout,
      transactionBufferWorkersThreadPoolAmount = transactionsBufferWorkersThreadPoolAmount,
      processingEngineWorkersThreadAmount = processingEngineWorkersThreadAmount,
      pollingFrequencyDelay = pollingFrequencyDelay,
      transactionsQueueBuilder = transactionsQueueBuilder)
}

