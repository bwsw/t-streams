package com.bwsw.tstreams.debug

import com.bwsw.tstreams.agents.producer.BasicProducerTransaction
import org.slf4j.LoggerFactory

/**
  * ONLY FOR DEBUGGING PURPOSES
  * Used for injecting events in runtime
  */
object GlobalHook {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val hooks = scala.collection.mutable.Map[String, () => Unit]()

  def addHook(name : String, event : () => Unit) : Unit = {
    hooks += (name -> event)
  }

  def invoke(name : String) : Unit = {
    if (System.getProperty("DEBUG") == "true" && hooks.contains(name)){
      val event = hooks(name)
      logger.debug(s"[GLOBALHOOK] called hook name:{$name}")
      event()
    }
  }

  def invokePreCheckpointFailure(txn : BasicProducerTransaction[_,_]) = {
    if (System.getProperty("DEBUG") == "true" &&
        System.getProperty("AfterPreCheckpointFailure") == "true") {
      txn.stopKeepAlive()
      throw new RuntimeException
    }
  }
}
