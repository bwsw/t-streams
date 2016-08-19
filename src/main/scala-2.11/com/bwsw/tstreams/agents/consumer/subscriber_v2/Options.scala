package com.bwsw.tstreams.agents.consumer.subscriber_v2

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.consumer.Offset.IOffset
import com.bwsw.tstreams.common.AbstractPolicy
import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.generator.IUUIDGenerator

/**
  * Created by ivan on 19.08.16.
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
  * @param threadPoolAmount
  * @param useLastOffset
  * @tparam T
  */
case class Options[T](val transactionsPreload: Int,
                      val dataPreload:            Int,
                      val converter:              IConverter[Array[Byte], T],
                      val readPolicy:             AbstractPolicy,
                      val offset:                 IOffset,
                      val txnGenerator:           IUUIDGenerator,
                      val agentAddress:           String,
                      val zkRootPath:             String,
                      val zkHosts:                List[InetSocketAddress],
                      val zkSessionTimeout:       Int,
                      val zkConnectionTimeout:    Int,
                      val threadPoolAmount:       Int      = 1,
                      val useLastOffset:          Boolean  = true)

