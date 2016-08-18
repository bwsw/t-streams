package com.bwsw.tstreams.common

import java.util.concurrent.{Callable, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import com.twitter.common.zookeeper.{DistributedLockImpl, DistributedLock}
import org.slf4j.Logger

import scala.util.Random

/**
  * Created by ivan on 02.08.16.
  */
object LockUtil {
  val randomGenerator = new Random

  def lockOrDie(l: ReentrantLock, lt: (Int, TimeUnit), logger: Option[Logger] = None): Unit = {
    if(!l.tryLock(lt._1, lt._2))
    {
      if (logger.isDefined)
        logger.get.error(s"Failed to get lock object ${l.toString} in ${lt._1} ${lt._2.toString}.")
      throw new IllegalStateException(s"Failed to get lock object in ${lt._1} ${lt._2.toString}.")
    } else {
      if (logger.isDefined)
        logger.get.debug(s"Lock object ${l.toString} received.")
    }
  }

  def withLockOrDieDo[RTYPE](l: ReentrantLock,
                             lt: (Int, TimeUnit),
                             logger: Option[Logger] = None,
                             lambda: () => RTYPE): RTYPE = {

    val lStartTime = System.currentTimeMillis()
    val token = randomGenerator.nextInt().toString

    if(!l.tryLock(lt._1, lt._2))
    {
      if (logger.isDefined)
        logger.get.error(s"Token ${token} / Lock ${l.toString} / Failed to get lock object ${l.toString} in ${lt._1} ${lt._2.toString}.")
      throw new IllegalStateException(s"Token ${token} / Lock ${l.toString} / Failed to get lock object in ${lt._1} ${lt._2.toString}.")
    } else {
      if (logger.isDefined && logger.get.isDebugEnabled)
        logger.get.debug(s"Token ${token} / Lock ${l.toString} / Lock object ${l.toString} received.")
    }
    val fStartTime = System.currentTimeMillis()
    try {
      // function
      val rv = lambda()
      // end function

      if (logger.isDefined && logger.get.isDebugEnabled) {
        val fEndTime = System.currentTimeMillis()
        logger.get.debug(s"Token ${token} / Lock ${l.toString} / Function inside of withLockOrDieDo took ${fEndTime - fStartTime} ms to run.")
      }

      l.unlock()

      if (logger.isDefined && logger.get.isDebugEnabled) {
        val lEndTime = System.currentTimeMillis()
        logger.get.debug(s"Token ${token} /Lock ${l.toString} / Section of withLockOrDieDo took ${lEndTime - lStartTime} ms to run.")
        logger.get.debug(s"Token ${token} /Lock ${l.toString} / Unlocked ${l.toString} in ${lt._1} ${lt._2.toString}.")
      }

      return rv
    } catch {
      case e: Exception =>
        l.unlock()
        if (logger.isDefined) {
          val fEndTime = System.currentTimeMillis();
          val lEndTime = System.currentTimeMillis()
          logger.get.debug(s"Token ${token} /Lock ${l.toString} / Function inside of withLockOrDieDo took ${fEndTime - fStartTime} ms to run. Resulted to exception.")
          logger.get.debug(s"Token ${token} /Lock ${l.toString} / Section of withLockOrDieDo took ${lEndTime - lStartTime} ms to run. Resulted to exception.")
          logger.get.error(s"Lock ${l.toString} / Exception is: ${e.toString}")
        }

        throw e
    }
  }

  def withZkLockOrDieDo[RTYPE](l: DistributedLockImpl,
                               lt: (Int, TimeUnit),
                               logger: Option[Logger] = None,
                               lambda: () => RTYPE): RTYPE = {

    val lStartTime = System.currentTimeMillis()
    val token = randomGenerator.nextInt().toString

    if(!l.tryLock(lt._1, lt._2))
    {
      if (logger.isDefined)
        logger.get.error(s"Token ${token} / Lock ${l.toString} / Failed to get lock object ${l.toString} in ${lt._1} ${lt._2.toString}.")
      throw new IllegalStateException(s"Token ${token} / Lock ${l.toString} / Failed to get lock object in ${lt._1} ${lt._2.toString}.")
    } else {
      if (logger.isDefined && logger.get.isDebugEnabled)
        logger.get.debug(s"Token ${token} / Lock ${l.toString} / Lock object ${l.toString} received.")
    }
    val fStartTime = System.currentTimeMillis()
    try {
      val rv = lambda()

      if (logger.isDefined && logger.get.isDebugEnabled) {
        val fEndTime = System.currentTimeMillis()
        logger.get.debug(s"Token ${token} / Lock ${l.toString} / Function inside of withLockOrDieDo took ${fEndTime - fStartTime} ms to run.")
      }

      l.unlock()

      if (logger.isDefined && logger.get.isDebugEnabled) {
        val lEndTime = System.currentTimeMillis()
        logger.get.debug(s"Token ${token} /Lock ${l.toString} / Section of withLockOrDieDo took ${lEndTime - lStartTime} ms to run.")
        logger.get.debug(s"Token ${token} /Lock ${l.toString} / Unlocked ${l.toString} in ${lt._1} ${lt._2.toString}.")
      }

      return rv
    } catch {
      case e: Exception =>
        l.unlock()
        if (logger.isDefined) {
          val fEndTime = System.currentTimeMillis()
          val lEndTime = System.currentTimeMillis()
          logger.get.debug(s"Token ${token} /Lock ${l.toString} / Function inside of withLockOrDieDo took ${fEndTime - fStartTime} ms to run. Resulted to exception.")
          logger.get.debug(s"Token ${token} /Lock ${l.toString} / Section of withLockOrDieDo took ${lEndTime - lStartTime} ms to run. Resulted to exception.")
          logger.get.error(s"Lock ${l.toString} / Exception is: ${e.getMessage}")
          logger.get.error("Stack trace is:", e)
        }

        throw e
    }
  }
}