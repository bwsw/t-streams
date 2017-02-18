package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.agents.consumer
import com.bwsw.tstreams.agents.consumer.subscriber.QueueBuilder.InMemory

object SubscriberOptionsBuilder {
  def fromConsumerOptions(consumerOpts: consumer.ConsumerOptions,
                          agentAddress: String,
                          zkPrefixPath: String,
                          zkEndpoints: String,
                          zkSessionTimeoutMs: Int,
                          zkConnectionTimeoutMs: Int,
                          transactionsBufferWorkersThreadPoolSize: Int = 1,
                          processingEngineWorkersThreadSize: Int = 1,
                          pollingFrequencyDelayMs: Int = 1000,
                          transactionsQueueBuilder: QueueBuilder.Abstract = new InMemory): SubscriberOptions =
    new SubscriberOptions(
      transactionsPreload = consumerOpts.transactionsPreload,
      dataPreload = consumerOpts.dataPreload,
      readPolicy = consumerOpts.readPolicy,
      offset = consumerOpts.offset,
      transactionGenerator = consumerOpts.transactionGenerator,
      useLastOffset = consumerOpts.useLastOffset,
      agentAddress = agentAddress,
      zkRootPath = zkPrefixPath,
      zkHosts = zkEndpoints,
      zkSessionTimeout = zkSessionTimeoutMs,
      zkConnectionTimeout = zkConnectionTimeoutMs,
      transactionBufferWorkersThreadPoolAmount = transactionsBufferWorkersThreadPoolSize,
      processingEngineWorkersThreadAmount = processingEngineWorkersThreadSize,
      pollingFrequencyDelay = pollingFrequencyDelayMs,
      transactionsQueueBuilder = transactionsQueueBuilder)
}

