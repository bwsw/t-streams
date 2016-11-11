package transactionService.impl

import java.io.File

import scala.concurrent.{Future => ScalaFuture}
import com.twitter.util.{Future => TwitterFuture}
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.twitter_util.UtilBijections._

import com.sleepycat.je.{Environment, EnvironmentConfig}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.sleepycat.persist.model._
import com.twitter.logging.{Level, Logger}
import transactionService.rpc._


trait TransactionMetaServiceImpl extends TransactionMetaService[TwitterFuture] {

  def putTransaction(token: String, transaction: Transaction): TwitterFuture[Boolean] = {
    val directory = new File(StreamServiceImpl.pathToDatabases)

    val environmentConfig = new EnvironmentConfig()
      .setAllowCreate(true)
    val storeConfig = new StoreConfig()
      .setAllowCreate(true)

    val environment = new Environment(directory, environmentConfig)
    val entityStore = new EntityStore(environment, TransactionMetaServiceImpl.storeName, storeConfig)

    val (producerTransactionOpt, consumerTransactionOpt) = (transaction.producerTransaction, transaction.consumerTransaction)

    val result = (producerTransactionOpt, consumerTransactionOpt) match {
      case (Some(txn), _) =>
        implicit val context = transactionService.impl.thread.Context.transactionContexts.getContext(txn.partition, txn.stream.toInt)
        ScalaFuture {
          val isNotExist =
            entityStore.getPrimaryIndex(
            classOf[TransactionMetaServiceImpl.ProducerTransactionKey],
            classOf[TransactionMetaServiceImpl.ProducerTransaction]
            ).putNoOverwrite(new TransactionMetaServiceImpl.ProducerTransaction(txn.transactionID, txn.state, txn.stream, txn.timestamp, txn.quantity, txn.partition, txn.tll))
          if (isNotExist) {
            TransactionMetaServiceImpl.logger.log(Level.INFO, s"${txn.toString} inserted to DB!")
            isNotExist
          } else {
            TransactionMetaServiceImpl.logger.log(Level.WARNING, s"${txn.toString} exists in DB!")
            isNotExist
          }
        }.as[TwitterFuture[Boolean]]
      case (_, Some(txn)) =>
        implicit val context = transactionService.impl.thread.Context.transactionContexts.getContext(0L)
        ScalaFuture(
          entityStore.getPrimaryIndex(
            classOf[TransactionMetaServiceImpl.ConsumerTransactionKey],
            classOf[TransactionMetaServiceImpl.ConsumerTransaction]
          ).putNoOverwrite(new TransactionMetaServiceImpl.ConsumerTransaction(txn.name, txn.stream, txn.partition, txn.transactionID))
        )(transactionService.impl.thread.Context.transactionContexts.getContext(txn.partition, txn.stream.toInt)).as[TwitterFuture[Boolean]]
      case _ =>
        implicit val context = transactionService.impl.thread.Context.transactionContexts.getContext(0L)
        ScalaFuture(false).as[TwitterFuture[Boolean]]
    }


    result flatMap {isInserted =>
      entityStore.close()
      environment.close()
      TwitterFuture.value(isInserted)
    }
  }

  override def putTransactions(token: String, transactions: Seq[Transaction]): TwitterFuture[Boolean] = {
    val directory = new File(StreamServiceImpl.pathToDatabases)
    val environmentConfig = new EnvironmentConfig()
      .setAllowCreate(true)
      .setTransactional(true)

    val storeConfig = new StoreConfig()
      .setAllowCreate(true)
      .setTransactional(true)

    val environment = new Environment(directory, environmentConfig)
    val entityStore = new EntityStore(environment, TransactionMetaServiceImpl.storeName, storeConfig)

    val producerPrimaryIndex = entityStore.getPrimaryIndex(
      classOf[TransactionMetaServiceImpl.ProducerTransactionKey],
      classOf[TransactionMetaServiceImpl.ProducerTransaction]
    )

    val consumerPrimaryIndex = entityStore.getPrimaryIndex(
      classOf[TransactionMetaServiceImpl.ConsumerTransactionKey],
      classOf[TransactionMetaServiceImpl.ConsumerTransaction]
    )

    val transactionDB = environment.beginTransaction(null, null)
    val result = transactions map { transaction =>
      (transaction.producerTransaction, transaction.consumerTransaction) match {
        case (Some(txn), _) =>
          implicit val context = transactionService.impl.thread.Context.transactionContexts.getContext(txn.partition, txn.stream.toInt)
          ScalaFuture {
            val isNotExist =
              entityStore.getPrimaryIndex(
                classOf[TransactionMetaServiceImpl.ProducerTransactionKey],
                classOf[TransactionMetaServiceImpl.ProducerTransaction]
              ).putNoOverwrite(new TransactionMetaServiceImpl.ProducerTransaction(txn.transactionID, txn.state, txn.stream, txn.timestamp, txn.quantity, txn.partition, txn.tll))
            if (isNotExist) {
              TransactionMetaServiceImpl.logger.log(Level.INFO, s"${txn.toString} inserted to DB!")
              isNotExist
            } else {
              TransactionMetaServiceImpl.logger.log(Level.WARNING, s"${txn.toString} exists in DB!")
              isNotExist
            }
          }.as[TwitterFuture[Boolean]]
        case (_, Some(txn)) =>
          implicit val context = transactionService.impl.thread.Context.transactionContexts.getContext(0L)
          ScalaFuture(
            entityStore.getPrimaryIndex(
              classOf[TransactionMetaServiceImpl.ConsumerTransactionKey],
              classOf[TransactionMetaServiceImpl.ConsumerTransaction]
            ).putNoOverwrite(new TransactionMetaServiceImpl.ConsumerTransaction(txn.name, txn.stream, txn.partition, txn.transactionID))
          )(transactionService.impl.thread.Context.transactionContexts.getContext(txn.partition, txn.stream.toInt)).as[TwitterFuture[Boolean]]
        case _ =>
          implicit val context = transactionService.impl.thread.Context.transactionContexts.getContext(0L)
          ScalaFuture(false).as[TwitterFuture[Boolean]]
      }
    }

    TwitterFuture.collect(result).flatMap { transactions =>
      transactionDB.commit()

      entityStore.close()
      environment.close()

      TwitterFuture.value(transactions.forall(_ == true))
    }
  }

//  def delTransaction(token: String, stream: String, partition: Int, transaction: Long): Future[Boolean] = {
//    val directory = new File(StreamServiceImpl.pathToDatabases)
//
//    val environmentConfig = new EnvironmentConfig()
//    val storeConfig = new StoreConfig()
//
//    val environment = new Environment(directory, environmentConfig)
//    val entityStore = new EntityStore(environment, TransactionMetaServiceImpl.storeName, storeConfig)
//
//    val result = Future {
//      val producerPrimaryKey = entityStore.getPrimaryIndex(
//        classOf[TransactionMetaServiceImpl.ProducerTransactionKey],
//        classOf[TransactionMetaServiceImpl.ProducerTransaction]
//      )
//      val txnOpt = Option(producerPrimaryKey.get(new TransactionMetaServiceImpl.ProducerTransactionKey(stream, partition, transaction)))
//      txnOpt match {
//        case Some(txn) => {
//          val txnToSave = new TransactionMetaServiceImpl.ProducerTransaction(txn.transactionID, TransactionStates.Invalid, txn.stream, txn.timestamp, txn.quantity, txn.partition)
//          if (producerPrimaryKey.put(txnToSave) != null) {
//            TransactionMetaServiceImpl.logger.log(Level.INFO, s"${txnToSave.toString} changes ${txn.state} state to ${TransactionStates.Invalid} state!")
//            true
//          } else {
//            TransactionMetaServiceImpl.logger.log(Level.ERROR, s"${txnToSave.toString}. Unexpected error.")
//            false
//          }
//        }
//        case None =>
//          TransactionMetaServiceImpl.logger.log(Level.WARNING, s"Producer transaction ${transaction.toString} doesn't exist!")
//          false
//      }
//    }
//
//    result flatMap { isMarked =>
//      entityStore.close()
//      environment.close()
//      Future.value(isMarked)
//    }
//  }

  def scanTransactions(token: String, stream: String, partition: Int): TwitterFuture[Seq[Transaction]] = ???

  def scanTransactionsCRC32(token: String, stream: String, partition: Int): TwitterFuture[Int] = ???
}

private object TransactionMetaServiceImpl {
  final val storeName = "TransactionStore"
  val logger = Logger.get()

  @Entity
  class ProducerTransaction extends transactionService.rpc.ProducerTransaction {
    @PrimaryKey var key: ProducerTransactionKey = _
    private var stateDB: Int = _
    private var timestampDB: java.lang.Long = _
    private var quantityDB: Int = _
    private var ttlDB: java.lang.Long = _

    def this(transactionID: java.lang.Long,
             state: TransactionStates,
             stream: String,
             timestamp: java.lang.Long,
             quantity: Int,
             partition: Int,
             ttl: Long) {
      this()
      this.stateDB = state.getValue()
      this.timestampDB = timestamp
      this.quantityDB = quantity
      this.ttlDB = ttl
      this.key = new ProducerTransactionKey(stream, partition, transactionID)
    }

    override def transactionID: Long = key.transactionID
    override def state: TransactionStates = TransactionStates(stateDB)
    override def stream: String = key.stream
    override def timestamp: Long = timestampDB
    override def quantity: Int = quantityDB
    override def partition: Int = key.partition
    override def tll: Long = ttlDB

    override def toString: String = {s"Producer transaction: ${key.toString}"}
  }

  @Persistent
  class ProducerTransactionKey {
    @KeyField(1) var stream: String = _
    @KeyField(2) var partition: Int = _
    @KeyField(3) var transactionID: java.lang.Long = _
    def this(stream: String, partition:Int, transactionID: java.lang.Long) = {
      this()
      this.stream = stream
      this.partition = partition
      this.transactionID = transactionID
    }

    override def toString: String = s"stream:$stream\tpartition:$partition\tid:$transactionID"
  }

  @Entity
  class ConsumerTransaction extends transactionService.rpc.ConsumerTransaction {
    @PrimaryKey var key: ConsumerTransactionKey = _
    var transactionIDDB: java.lang.Long = _

    def this(name: String, stream: String, partition:Int, transactionID: java.lang.Long) = {
      this()
      this.transactionIDDB = transactionID
      this.key = new ConsumerTransactionKey(name, stream, partition)
    }

    override def transactionID: Long = transactionIDDB
    override def stream: String = key.stream
    override def partition: Int = key.partition
    override def name: String = key.name
  }

  @Persistent
  class ConsumerTransactionKey {
    @KeyField(1) var name: String = _
    @KeyField(2) var stream: String = _
    @KeyField(3) var partition: Int = _
    def this(name: String, stream: String, partition:Int) = {
      this()
      this.name = name
      this.stream = stream
      this.partition = partition
    }

    override def toString: String = s"$name $stream $partition"
  }
}
