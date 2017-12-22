package com.bwsw.tstreamstransactionserver

import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionKey, ProducerTransactionValue}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions
import org.json4s.jackson.JsonMethods.{pretty, render}
import org.json4s.jackson.Serialization
import org.json4s.{Extraction, NoTypeHints}

import scala.collection.mutable.ArrayBuffer

object DumpProcessedAllTransactionsUtility {

  implicit val formatsTransaction = Serialization.formats(NoTypeHints)

  def main(args: Array[String]): Unit = {
    if (args.length < 2)
      throw new IllegalArgumentException(
        "Path to database folder and name of transaction metadata database folder name should be provided."
      )
    else {
      val rocksStorage = new MultiAndSingleNodeRockStorage(
        SingleNodeServerOptions.StorageOptions(
          path = args(0),
          metadataDirectory = args(1)
        ),
        SingleNodeServerOptions.RocksStorageOptions(
          transactionExpungeDelayMin = -1
        ),
        readOnly = true
      )
      val database =
        rocksStorage.getStorageManager.getDatabase(Storage.TRANSACTION_ALL_STORE)

      val iterator = database.iterator
      iterator.seekToFirst()

      val records = new ArrayBuffer[ProducerTransactionRecordWrapper]()
      while (iterator.isValid) {
        val key = ProducerTransactionKey.fromByteArray(iterator.key)
        val value = ProducerTransactionValue.fromByteArray(iterator.value())

        records += ProducerTransactionRecordWrapper(
          key.stream,
          key.partition,
          key.transactionID,
          value.state.value,
          value.quantity,
          value.ttl,
          value.timestamp
        )

        iterator.next()
      }

      val json = pretty(render(Extraction.decompose(Records(records))))
      println(json)

      iterator.close()
    }
  }

  private case class ProducerTransactionRecordWrapper(stream: Int,
                                                      partition: Int,
                                                      transaction: Long,
                                                      state: Int,
                                                      quantity: Int,
                                                      ttl: Long,
                                                      timestamp: Long
                                                     )

  private case class Records(records: Seq[ProducerTransactionRecordWrapper])

}
