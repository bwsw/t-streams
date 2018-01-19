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

import java.nio.file.{Path, Paths}
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.CompactionJob
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.storage.BookKeeperWrapper
import com.bwsw.tstreamstransactionserver.options.CommonOptions


/**
  * Periodically runs BokKeeper compaction.
  * It's need to delete expired entries from BookKeeper (entry logs) and Zookeeper (ledgers meta).
  *
  * @param trees      contains ledgers meta
  * @param bookKeeper allows managing ledgers
  * @param ttl        the lifetime of a ledger in seconds
  * @param interval   time interval between compactions
  * @param timeUnit   time unit of time interval
  */
class BookKeeperCompactionJob(trees: Array[LongZookeeperTreeList],
                              bookKeeper: BookKeeperWrapper,
                              ttl: Long,
                              interval: Long,
                              timeUnit: TimeUnit = CommonOptions.COMPACTION_TIME_UNIT)
  extends CompactionJob(interval, timeUnit) {

  override def compact(): Unit = {
    trees.foreach(tree => {
      tree.firstEntityId match {
        case Some(firstLedger) =>
          deleteExpiredLedgers(tree, firstLedger)
        case None =>
          logger.info(s"There were no expired ledgers")
      }
    })
  }

  private def deleteExpiredLedgers(tree: LongZookeeperTreeList, firstLedger: Long): Unit = {

    def inner(currentLedger: Long): Unit = {
      if (logger.isDebugEnabled) logger.debug(s"Try to delete a ledger (id = $currentLedger)")
      bookKeeper.openLedger(currentLedger) match {
        case Some(ledger) if toSeconds(ledger.getCreationTime) + ttl < toSeconds(System.currentTimeMillis()) =>
          bookKeeper.deleteLedger(currentLedger)
          tree.deleteNode(currentLedger)
          if (logger.isDebugEnabled) logger.debug(s"A ledger (id = $currentLedger) has been deleted")
          tree.firstEntityId match {
            case Some(nextLedger) =>
              inner(nextLedger)
            case _ =>
              if (logger.isDebugEnabled) logger.debug("Finished the deletion process of expired ledgers")
          }
        case Some(_) =>
          if (logger.isDebugEnabled) logger.debug(s"A ledger (id = $currentLedger) is not expired. " +
            s"Finished the deletion process of expired ledgers")
        case None =>
          tree.deleteNode(currentLedger)
          logger.warn(s"There is a ledger (id = $currentLedger) in ZookeeperTreeList that doesn't exist in bookkeeper. " +
            s"Delete only znode")
      }
    }

    inner(firstLedger)
  }

  private def toSeconds(ms: Long) = {
    TimeUnit.MILLISECONDS.toSeconds(ms)
  }
}
