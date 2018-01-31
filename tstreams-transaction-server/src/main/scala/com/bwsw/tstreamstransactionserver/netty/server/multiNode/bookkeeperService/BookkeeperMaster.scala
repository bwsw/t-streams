/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.bwsw.tstreamstransactionserver.exception.Throwable.ServerIsSlaveException
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.storage.BookKeeperWrapper
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKIDGenerator

import scala.annotation.tailrec
import scala.util.Try


class BookkeeperMaster(bookKeeper: BookKeeperWrapper,
                       zkLastClosedLedgerHandler: ZKIDGenerator,
                       master: LeaderSelectorInterface,
                       zkTreeListLedger: LongZookeeperTreeList,
                       timeBetweenCreationOfLedgers: Int)
  extends Runnable {

  private val lock = new ReentrantReadWriteLock()
  @volatile private var currentOpenedLedger: Option[LedgerHandle] = None


  private def closeLastLedger(): Unit = {
    zkTreeListLedger
      .lastEntityId
      .foreach { id =>
        bookKeeper.openLedger(id).foreach(closeLedger)
      }
  }

  private def closeLedger(ledgerHandle: LedgerHandle): Unit = {
    Try {
      ledgerHandle.close()
    }
  }

  private final def whileLeaderDo() = {

    var lastAccessTimes = 0L

    @tailrec
    def onBeingLeaderDo(): Unit = {
      if (master.hasLeadership) {
        val diff = System.currentTimeMillis() - lastAccessTimes
        if (diff < timeBetweenCreationOfLedgers) {
          val timeToWait = timeBetweenCreationOfLedgers - diff
          TimeUnit.MILLISECONDS.sleep(timeToWait)
          onBeingLeaderDo()
        }
        else {
          lastAccessTimes = System.currentTimeMillis()
          Try {
            bookKeeper.createLedger(System.currentTimeMillis())
          }.map { ledgerHandle =>

            zkTreeListLedger.createNode(
              ledgerHandle.id
            )
            val maybePreviousOpenedLedger = currentOpenedLedger
            lock.writeLock().lock()
            try {
              currentOpenedLedger = Some(ledgerHandle)
            }
            finally {
              lock.writeLock().unlock()
            }

            maybePreviousOpenedLedger.foreach { previousOpenedLedger =>
              while (
                previousOpenedLedger.lastEnqueuedRecordId !=
                  previousOpenedLedger.lastRecordID()
              ) {}
              closeLedger(previousOpenedLedger)
              zkLastClosedLedgerHandler
                .setID(previousOpenedLedger.id)
            }
          }
          onBeingLeaderDo()
        }
      }
    }

    onBeingLeaderDo()
  }


  @tailrec
  private def retryToGetLedger: Either[ServerIsSlaveException, LedgerHandle] = {
    currentOpenedLedger match {
      case None =>
        if (master.hasLeadership) {
          TimeUnit.MILLISECONDS.sleep(10)
          retryToGetLedger
        }
        else
          Left(new ServerIsSlaveException)

      case Some(openedLedger) =>
        Right(openedLedger)
    }
  }

  private def lead(): Unit = {
    closeLastLedger()
    whileLeaderDo()
  }

  @throws[Exception]
  def doOperationWithCurrentWriteLedger[T](operate: Either[ServerIsSlaveException, LedgerHandle] => T): T = {

    if (master.hasLeadership) {
      lock.readLock().lock()
      try {
        val ledgerHandle = retryToGetLedger
        operate(ledgerHandle)
      }
      catch {
        case throwable: Throwable =>
          throw throwable
      }
      finally {
        lock.readLock().unlock()
      }
    } else {
      operate(Left(new ServerIsSlaveException))
    }
  }

  override def run(): Unit = {
    try {
      while (true) {
        if (master.hasLeadership)
          lead()
        else {
          currentOpenedLedger match {
            case Some(openedLedger) =>
              currentOpenedLedger = None
              openedLedger.close()
            case _ =>
          }
        }
      }
    }
    catch {
      case _: java.lang.InterruptedException =>
        Thread.currentThread().interrupt()
    }
  }
}
