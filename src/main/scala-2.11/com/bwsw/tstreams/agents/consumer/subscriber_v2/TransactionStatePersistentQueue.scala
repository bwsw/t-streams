package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.common.AbstractPersistentQueue

/**
  * Created by ivan on 19.08.16.
  */
class TransactionStatePersistentQueue(basePath: String)
  extends AbstractPersistentQueue[List[TransactionState]](basePath: String) {

  def serialize(elt: List[TransactionState]): Unit = {

  }

  def deserialize(data: String): List[TransactionState] = {
    Nil
  }

}
