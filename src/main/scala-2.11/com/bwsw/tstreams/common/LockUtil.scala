package com.bwsw.tstreams.common

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.twitter.common.zookeeper.DistributedLockImpl
import org.slf4j.Logger

import scala.util.Random

/**
  * Created by ivan on 02.08.16.
  */
object LockUtil {
  private val randomGenerator = new Random

  /**
    * Try acquire lock or throw exception in case of failure
    *
    * @param l lock
    * @param lt time amount and time unit
    * @param logger
    */
  def lockOrDie(l: ReentrantLock,
                lt: (Int, TimeUnit),
                logger: Option[Logger] = None): Unit = {
    val (amount, timeUnit) = lt
    if(!l.tryLock(amount, timeUnit)) {
      logger.foreach(l => l.error(s"Failed to get lock object ${l.toString} in $amount $timeUnit."))
      throw new LockUtilException(s"Failed to get lock object in $amount $timeUnit.")
    } else
      logger.foreach(l => l.debug(s"Lock object ${l.toString} received."))
  }

  /**
    * Try execute lambda with lock or throw exception in case of failure
    *
    * @param l lock
    * @param lt time amount and time unit
    * @param logger
    * @param lambda
    * @tparam T
    * @return
    */
  def withLockOrDieDo[T](l: ReentrantLock,
                        lt: (Int, TimeUnit),
                        logger: Option[Logger] = None,
                        lambda: () => T): T = {

    val lStartTime = System.currentTimeMillis()
    val token = randomGenerator.nextInt().toString
    val (amount, timeUnit) = lt

    //acquire lock or throw exception
    if(!l.tryLock(amount, timeUnit)) {
      logger.foreach(l =>
        l.error(s"Token $token / Lock ${l.toString} / Failed to get lock object ${l.toString} in $amount ${timeUnit.toString}."))
      throw new LockUtilException(s"Token $token / Lock ${l.toString} / Failed to get lock object in $amount ${timeUnit.toString}.")
    } else {
      logger.foreach(l =>
        if (l.isDebugEnabled)
          l.debug(s"Token $token / Lock ${l.toString} / Lock object ${l.toString} received."))
    }

    val fStartTime = System.currentTimeMillis()
    // try execute lambda, throw exception in case of failure
    try {
      val rv: T = lambda()
      logger.foreach(l =>
        if (l.isDebugEnabled){
          val fEndTime = System.currentTimeMillis()
          l.debug(s"Token $token / Lock ${l.toString} / Function inside of withLockOrDieDo took ${fEndTime - fStartTime} ms to run.")
        })

      //unlock acquired lock
      l.unlock()

      logger.foreach(l =>
        if (l.isDebugEnabled){
          val lEndTime = System.currentTimeMillis()
          l.debug(s"Token $token /Lock ${l.toString} / Section of withLockOrDieDo took ${lEndTime - lStartTime} ms to run.")
          l.debug(s"Token $token /Lock ${l.toString} / Unlocked ${l.toString} in ${lt._1} ${lt._2.toString}.")
        })

      return rv
    }
    catch {
      case e: Exception =>
        //unlock acquired lock to prevent deadlock of further execution
        l.unlock()

        logger.foreach{l =>
            val fEndTime = System.currentTimeMillis()
            val lEndTime = System.currentTimeMillis()
            l.debug(s"Token $token /Lock ${l.toString} / Function inside of withLockOrDieDo took" +
              s" ${fEndTime - fStartTime} ms to run. Resulted to exception.")
            l.debug(s"Token $token /Lock ${l.toString} / Section of withLockOrDieDo" +
              s" took ${lEndTime - lStartTime} ms to run. Resulted to exception.")
            l.error(s"Lock ${l.toString} / Exception is: ${e.toString}")
        }

        throw e
    }
  }

  /**
    * Try execute lambda with distributed Zookeeper lock
    * or throw exception in case of failure
    *
    * @param l lock
    * @param lt time amount and time unit
    * @param logger
    * @param lambda
    * @tparam T
    * @return
    */
  def withZkLockOrDieDo[T](l: DistributedLockImpl,
                           lt: (Int, TimeUnit),
                           logger: Option[Logger] = None,
                           lambda: () => T): T = {

    val lStartTime = System.currentTimeMillis()
    val token = randomGenerator.nextInt().toString
    val (amount, timeUnit) = lt

    //acquire lock or throw exception
    if(!l.tryLock(amount, timeUnit)) {
      logger.foreach(l =>
        l.error(s"Token $token / Lock ${l.toString} / Failed to get lock object ${l.toString} in $amount ${timeUnit.toString}."))
      throw new LockUtilException(s"Token ${token} / Lock ${l.toString} / Failed to get lock object in $amount ${timeUnit.toString}.")
    } else {
      logger.foreach(l =>
        if (l.isDebugEnabled)
          l.debug(s"Token $token / Lock ${l.toString} / Lock object ${l.toString} received."))
    }

    val fStartTime = System.currentTimeMillis()
    // try execute lambda, throw exception in case of failure
    try {
      val rv = lambda()

      logger.foreach(l =>
        if (l.isDebugEnabled) {
          val fEndTime = System.currentTimeMillis()
          l.debug(s"Token $token / Lock ${l.toString} / Function inside of withLockOrDieDo took ${fEndTime - fStartTime} ms to run.")
        })

      //unlock acquired lock
      l.unlock()

      logger.foreach(l =>
        if (l.isDebugEnabled){
          val lEndTime = System.currentTimeMillis()
          l.debug(s"Token $token /Lock ${l.toString} / Section of withLockOrDieDo took ${lEndTime - lStartTime} ms to run.")
          l.debug(s"Token $token /Lock ${l.toString} / Unlocked ${l.toString} in $amount ${timeUnit.toString}.")
        })

      return rv
    } catch {
      case e: Exception =>
        //unlock acquired lock to prevent deadlock of further execution
        l.unlock()

        logger.foreach { l =>
          val fEndTime = System.currentTimeMillis()
          val lEndTime = System.currentTimeMillis()
          l.debug(s"Token $token /Lock ${l.toString} / Function inside of withLockOrDieDo " +
            s"took ${fEndTime - fStartTime} ms to run. Resulted to exception.")
          l.debug(s"Token $token /Lock ${l.toString} / Section of withLockOrDieDo " +
            s"took ${lEndTime - lStartTime} ms to run. Resulted to exception.")
          l.error(s"Lock ${l.toString} / Exception is: ${e.toString}")
        }

        throw e
    }
  }

  /**
    *
    * @param msg
    */
  case class LockUtilException(msg : String) extends Exception(msg)
}
