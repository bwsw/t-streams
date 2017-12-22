package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{LedgerHandle, LedgerManager}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordWithIndex, TimestampRecord}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZkMultipleTreeListReader._
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata._


private object ZkMultipleTreeListReader {
  private val NoLedgerExist: Long = -1L

  private val NoRecordRead: Long = -1L
}

class ZkMultipleTreeListReader(zkTreeLists: Array[LongZookeeperTreeList],
                               lastClosedLedgers: Array[LongNodeCache],
                               storageManager: LedgerManager) {

  private type Timestamp = Long

  def read(processedLastRecordIDsAcrossLedgers: Array[LedgerMetadata]): (Array[Record], Array[LedgerMetadata]) = {
    val processedLastRecordIDsAcrossLedgersCopy =
      java.util.Arrays.copyOf(
        processedLastRecordIDsAcrossLedgers,
        processedLastRecordIDsAcrossLedgers.length
      )

    val nextRecordsAndLedgersToProcess =
      getNextLedger(
        zkTreeLists zip getRecordsToStartWith(processedLastRecordIDsAcrossLedgersCopy)
      )
    if (
      nextRecordsAndLedgersToProcess.exists(metadata =>
        NoLedgerExist == metadata.id || MoveToNextLedgerStatus == metadata.metadataStatus)
    ) {
      (Array.empty[Record], processedLastRecordIDsAcrossLedgersCopy)
    }
    else {
      val (ledgersRecords, ledgersAndTheirLastRecordIDs) =
        getLedgerIDAndItsOrderedRecords(
          nextRecordsAndLedgersToProcess
        ).unzip

      val timestamp: Long =
        findMinMaxTimestamp(ledgersRecords)


      val (records, processedLedgersAndRecords) = {
        (ledgersRecords zip ledgersAndTheirLastRecordIDs).map {
          case (ledgerRecords, metadata) =>
            val recordsWithIndexes =
              ledgerRecords.takeWhile(_.record.timestamp <= timestamp)

            val (lastRecordID, newStatus): (Long, LedgerMetadataStatus) =
              recordsWithIndexes
                .lastOption
                .map(recordWithIndex => (recordWithIndex.index, IsOkayStatus))
                .getOrElse((metadata.lastRecordID, NoRecordProcessedStatus))

            val updatedMetadata =
              LedgerMetadata(metadata.id, lastRecordID, newStatus)

            (recordsWithIndexes.map(_.record), updatedMetadata)
        }.unzip
      }


      val nextLedgersForProcessing =
        getNextLedgersIfNecessary(processedLedgersAndRecords)

      (records.flatten, nextLedgersForProcessing)
    }
  }


  private def getNextLedger(ledgersMetadata: Array[(LongZookeeperTreeList, LedgerMetadata)]): Array[LedgerMetadata] = {
    ledgersMetadata.zip(lastClosedLedgers).map {
      case ((zkTreeList, ledgerMetadata), lastClosedLedgerHandler) =>
        if (ledgerMetadata.metadataStatus == MoveToNextLedgerStatus) {
          zkTreeList
            .getNextNode(ledgerMetadata.id)
            .filter(nextLedgerId =>
              nextLedgerId <= lastClosedLedgerHandler.getId
            )
            .map(newId => LedgerMetadata(newId, NoRecordRead, NoRecordReadStatus))
            .getOrElse(ledgerMetadata)
        }
        else {
          ledgerMetadata
        }
    }
  }

  @throws[IllegalArgumentException]
  private def getRecordsToStartWith(databaseData: Array[LedgerMetadata]): Array[LedgerMetadata] = {
    if (databaseData.nonEmpty) {
      require(
        databaseData.length == zkTreeLists.length,
        "Number of trees has been changed since last processing!"
      )
      databaseData
    }
    else {
      zkTreeLists.zip(lastClosedLedgers)
        .map { case (zkTreeList, lastClosedLedgerHandler) =>
          zkTreeList.firstEntityID
            .map { id =>
              if (id >= 0 && lastClosedLedgerHandler.getId >= 0) {
                LedgerMetadata(id, NoRecordRead, NoRecordReadStatus)
              }
              else
                LedgerMetadata(NoLedgerExist, NoRecordRead, NoRecordReadStatus)
            }
            .getOrElse(LedgerMetadata(NoLedgerExist, NoRecordRead, NoRecordReadStatus))
        }
    }
  }

  @throws[Throwable]
  private def findMinMaxTimestamp(recordsToProcess: Array[Array[RecordWithIndex]]): Timestamp = {
    val maxTimestamps =
      recordsToProcess
        .map(_.maxBy(_.record.timestamp))
        .map(_.record.timestamp)

    maxTimestamps.min
  }


  private def getLedgerIDAndItsOrderedRecords(ledgersAndTheirLastRecordsToProcess: Array[LedgerMetadata]) = {
    ledgersAndTheirLastRecordsToProcess
      .map { ledgerMetaInfo =>
        storageManager
          .openLedger(ledgerMetaInfo.id)
          .map { ledgerHandle =>
            val record =
              RecordWithIndex(NoRecordRead, new TimestampRecord(ledgerHandle.getCreationTime))

            val recordsWithIndexes =
              ledgerHandle.getOrderedRecords(ledgerMetaInfo.lastRecordID + 1)

            if (ledgerMetaInfo.lastRecordID < 0) {
              (record +: recordsWithIndexes, ledgerMetaInfo)
            } else {
              (recordsWithIndexes, ledgerMetaInfo)
            }
          }
          .getOrElse(throw new
              IllegalStateException(
                s"There is problem with storage - there is no such ledger ${ledgerMetaInfo.id}"
              )
          )
      }
  }


  private def areAllCurrentLedgerRecordsRead(metadata: LedgerMetadata,
                                             ledgerHandle: LedgerHandle): Boolean = {
    ledgerHandle.id == metadata.id &&
      ledgerHandle.lastRecordID() == metadata.lastRecordID &&
      metadata.metadataStatus != NoRecordProcessedStatus
  }

  private def getNextLedgersIfNecessary(lastRecordsAcrossLedgers: Array[LedgerMetadata]) = {
    lastRecordsAcrossLedgers.map { savedToDbLedgerMetadata =>
      val newLedgerIDOpt =
        for {
          currentLedgerHandle <- storageManager.openLedger(savedToDbLedgerMetadata.id)
          if areAllCurrentLedgerRecordsRead(
            savedToDbLedgerMetadata,
            currentLedgerHandle
          )
        } yield {
          LedgerMetadata(
            savedToDbLedgerMetadata.id,
            savedToDbLedgerMetadata.lastRecordID,
            MoveToNextLedgerStatus
          )
        }

      newLedgerIDOpt
        .getOrElse(savedToDbLedgerMetadata)
    }
  }
}
