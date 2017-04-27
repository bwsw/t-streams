package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, HasEnvironment}
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{Batch, RocksDBALL}
import org.slf4j.LoggerFactory

import scala.concurrent.{Future => ScalaFuture}

trait ConsumerServiceImpl extends Authenticable with ConsumerTransactionStateNotifier {
  val executionContext: ServerExecutionContext
  val rocksMetaServiceDB: RocksDBALL

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val consumerDatabase = rocksMetaServiceDB.getDatabase(HasEnvironment.CONSUMER_STORE)

  def getConsumerState(name: String, stream: String, partition: Int): ScalaFuture[Long] =
    ScalaFuture {
      val streamNameAsLong = getMostRecentStream(stream).id
      val consumerTransactionKey = ConsumerTransactionKey(name, streamNameAsLong, partition).toByteArray
      val consumerTransactionValue = Option(consumerDatabase.get(consumerTransactionKey))
      consumerTransactionValue.map(bytes =>
        ConsumerTransactionValue.fromByteArray(bytes).transactionId
      ).getOrElse{
        if (logger.isDebugEnabled()) logger.debug(s"There is no checkpointed consumer transaction on stream $name, partition $partition with name: $name. Returning -1L")
        -1L
      }
    }(executionContext.berkeleyReadContext)


  private final def transitConsumerTransactionToNewState(commitLogTransactions: Seq[ConsumerTransactionRecord]): ConsumerTransactionRecord = {
    commitLogTransactions.maxBy(_.timestamp)
  }

  private final def groupProducerTransactions(consumerTransactions: Seq[ConsumerTransactionRecord]) = {
    consumerTransactions.groupBy(txn => txn.key)
  }

  def putConsumersCheckpoints(consumerTransactions: Seq[ConsumerTransactionRecord], batch: Batch): Unit =
  {
    if (logger.isDebugEnabled()) logger.debug(s"Trying to commit consumer transactions: $consumerTransactions")
    groupProducerTransactions(consumerTransactions) foreach {case (key, txns) =>
      val theLastStateTransaction = transitConsumerTransactionToNewState(txns)
      val consumerTransactionValueBinary = theLastStateTransaction.consumerTransaction.toByteArray
      val consumerTransactionKeyBinary = key.toByteArray
      batch.put(HasEnvironment.CONSUMER_STORE, consumerTransactionKeyBinary, consumerTransactionValueBinary)
      if (areThereAnyConsumerNotifies) tryCompleteConsumerNotify(theLastStateTransaction)
    }
  }
}