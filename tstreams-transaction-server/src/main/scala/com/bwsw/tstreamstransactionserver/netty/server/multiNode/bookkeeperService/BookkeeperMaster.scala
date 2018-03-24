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

import com.bwsw.tstreamstransactionserver.exception.Throwable.{MasterIsNotReadyException, ServerIsSlaveException}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.storage.BookKeeperWrapper
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKIDGenerator

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}


class BookkeeperMaster(bookKeeper: BookKeeperWrapper,
                       zkLastClosedLedgerHandler: ZKIDGenerator,
                       master: LeaderSelectorInterface,
                       zkTreeListLedger: LongZookeeperTreeList,
                       timeBetweenCreationOfLedgers: Int)
  extends Runnable {

  private val lock = new ReentrantReadWriteLock()
  @volatile private var currentOpenedLedger: Option[LedgerHandle] = None
  private val waitUntilLedgerWritesPendingRecordsTimeout = 50


  private def closeLastLedger(): Unit = {
    zkTreeListLedger
      .lastEntityId
      .foreach { id =>
        bookKeeper.openLedger(id).foreach(closeLedger)
      }
  }

  private def closeLedger(ledgerHandle: LedgerHandle): Unit = Try(ledgerHandle.close())

  private final def whileLeaderDo() = {

    var lastAccessTime = 0L

    @tailrec
    def onBeingLeaderDo(): Unit = {
      if (master.hasLeadership) {
        val diff = System.currentTimeMillis() - lastAccessTime
        if (diff < timeBetweenCreationOfLedgers) {
          val timeToWait = timeBetweenCreationOfLedgers - diff
          TimeUnit.MILLISECONDS.sleep(timeToWait)
          onBeingLeaderDo()
        }
        else {
          lastAccessTime = System.currentTimeMillis()
          Try {
            bookKeeper.createLedger(System.currentTimeMillis())
          }.map { ledgerHandle =>

            zkTreeListLedger.createNode(ledgerHandle.id)
            val maybePreviousOpenedLedger = currentOpenedLedger
            lock.writeLock().lock()
            currentOpenedLedger = Some(ledgerHandle)
            lock.writeLock().unlock()

            maybePreviousOpenedLedger.foreach { previousOpenedLedger =>
              while (ledgerHandle.lastEnqueuedRecordId != ledgerHandle.lastRecordID()) {
                Thread.sleep(waitUntilLedgerWritesPendingRecordsTimeout)
              }
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


  private def lead(): Unit = {
    closeLastLedger()
    whileLeaderDo()
  }

  @throws[Exception]
  def doOperationWithCurrentWriteLedger[T](operate: Either[Exception, LedgerHandle] => T): T = {
    if (master.hasLeadership) {
      lock.readLock().lock()
      val result = Try {
        currentOpenedLedger match {
          case Some(ledgerHandle) => operate(Right(ledgerHandle))
          case None => operate(Left(new MasterIsNotReadyException))
        }
      }
      lock.readLock().unlock()

      result.get
    } else {
      operate(Left(new ServerIsSlaveException))
    }
  }

  override def run(): Unit = {
    val result = Try {
      while (true) {
        if (master.hasLeadership)
          lead()
        else {
          closeCurrentOpenedLedger()
          //          Thread.sleep(timeBetweenCreationOfLedgers)
        }
      }
    }

    Try(closeCurrentOpenedLedger())

    result match {
      case Success(_) =>
      case Failure(exception: InterruptedException) =>
        exception.printStackTrace()
        Thread.currentThread().interrupt()
      case Failure(exception: Throwable) =>
        exception.printStackTrace()
        throw exception
    }
  }


  private def closeCurrentOpenedLedger(): Unit = {
    currentOpenedLedger match {
      case Some(openedLedger) =>
        currentOpenedLedger = None
        openedLedger.close()
      case _ =>
    }
  }
}
