package com.bwsw.tstreams.debug

import org.slf4j.LoggerFactory

//TODO reengineering
/**
  * ONLY FOR DEBUGGING PURPOSES
  * Used for injecting events in runtime
  */
object GlobalHooks {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val hooks = scala.collection.mutable.Map[String, () => Unit]()

  def addHook(name: String, event: () => Unit): Unit = {
    hooks += (name -> event)
  }

  def invoke(name: String): Unit = {
    if (System.getProperty("DEBUG") == "true" && hooks.contains(name)) {
      val event = hooks(name)
      logger.debug(s"[GLOBALHOOK] called hook name:{$name}")
      event()
    }
  }

  def clear(): Unit = {
    hooks.clear()
  }

  def preCommitFailure = "PreCommitFailure"
  def afterCommitFailure = "AfterCommitFailure"
}
