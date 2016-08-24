package com.bwsw.tstreams.agents.consumer.subscriber_v2

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.consumer
import com.bwsw.tstreams.agents.consumer.subscriber_v2.QueueBuilder.InMemory

object OptionsBuilder {
  def fromConsumerOptions[T](consumerOpts: consumer.Options[T],
                             agentAddress: String,
                             zkRootPath: String,
                             zkHosts: Set[InetSocketAddress],
                             zkSessionTimeout:       Int,
                             zkConnectionTimeout:    Int,
                             txnBufferWorkersThreadPoolAmount:       Int      = 1,
                             processingEngineWorkersThreadAmount:    Int      = 1,
                             pollingFrequencyDelay:                  Int      = 1000,
                             txnQueueBuilder:                        QueueBuilder.Abstract  = new InMemory): Options[T] =
    return new Options[T](
      transactionsPreload = consumerOpts.transactionsPreload,
      dataPreload         = consumerOpts.dataPreload,
      converter           = consumerOpts.converter,
      readPolicy          = consumerOpts.readPolicy,
      offset              = consumerOpts.offset,
      txnGenerator        = consumerOpts.txnGenerator,
      useLastOffset       = consumerOpts.useLastOffset,
      agentAddress        = agentAddress,
      zkRootPath          = zkRootPath,
      zkHosts             = zkHosts,
      zkSessionTimeout    = zkSessionTimeout,
      zkConnectionTimeout = zkConnectionTimeout,
      txnBufferWorkersThreadPoolAmount    = txnBufferWorkersThreadPoolAmount,
      processingEngineWorkersThreadAmount = processingEngineWorkersThreadAmount,
      pollingFrequencyDelay               = pollingFrequencyDelay,
      txnQueueBuilder                     = txnQueueBuilder)
}

