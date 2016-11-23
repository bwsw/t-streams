package transactionService.client

import java.nio.ByteBuffer

import authService.ClientAuth

import scala.concurrent.{Future => ScalaFuture}
import com.twitter.util.{Await, Duration, Monitor, Throw, Time, Try, Future => TwitterFuture}
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.twitter_util.UtilBijections._
import com.twitter.finagle.service._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Failure, Thrift}
import com.twitter.logging.{Level, Logger}
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, Stream, Transaction, TransactionService, TransactionStates}
import com.twitter.conversions.time._
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.service.exp.FailureAccrualPolicy

import scala.collection.mutable.ArrayBuffer


class ClientTransaction(serverIPAddress: String)/*(implicit val threadPool: transactionService.Context)*/ extends TransactionService[TwitterFuture] {
  private val client = Thrift.client
    .configured(FailureAccrualFactory.Param(() => FailureAccrualPolicy.successRate(
      requiredSuccessRate = 0.00,
      window = 100,
      markDeadFor = Backoff.const(10.seconds)
    )))
    .withSessionQualifier.noFailFast
    .withTransport.connectTimeout(1.minute)

  private val interface= client.newServiceIface[TransactionService.ServiceIface](serverIPAddress, "transaction")
  private val interfaceCopy = interface.copy(
    putStream =             interface.putStream,
    isStreamExist =         interface.isStreamExist,
    getStream =             interface.getStream,
    delStream =             interface.delStream,
    putTransaction =        interface.putTransaction,
    putTransactions =       interface.putTransactions,
    scanTransactions =      interface.scanTransactions,
    putTransactionData =    interface.putTransactionData,
    getTransactionData =    interface.getTransactionData,
    setConsumerState =      interface.setConsumerState,
    getConsumerState =      interface.getConsumerState
  )
  private val request = Thrift.client.newMethodIface(interfaceCopy)

  //Stream API
  override def putStream(token: String, stream: String, partitions: Int, description: Option[String]): TwitterFuture[Boolean] = {
    request.putStream(token, stream, partitions, description)
  }
  override def isStreamExist(token: String, stream: String): TwitterFuture[Boolean] = request.isStreamExist(token, stream)
  override def getStream(token: String, stream: String): TwitterFuture[Stream]  = request.getStream(token, stream)
  override def delStream(token: String, stream: String): TwitterFuture[Boolean] = request.delStream(token, stream)

  //TransactionMeta API
  override def putTransaction(token: String, transaction: Transaction): TwitterFuture[Boolean] = {
    Await.ready(request.putTransaction(token, transaction))
  }
  override def putTransactions(token: String, transactions: Seq[Transaction]): TwitterFuture[Boolean] = {
    Await.ready(request.putTransactions(token, transactions))
  }
  override def scanTransactions(token: String, stream: String, partition: Int): TwitterFuture[Seq[Transaction]] = request.scanTransactions(token, stream, partition)

  //TransactionData API
  override def putTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, data: Seq[ByteBuffer]): TwitterFuture[Boolean] =
    request.putTransactionData(token, stream, partition, transaction, from, data)
  override def getTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int): TwitterFuture[Seq[ByteBuffer]] =
    request.getTransactionData(token,stream,partition,transaction,from,to)

  //Consumer API
  override def setConsumerState(token: String, name: String, stream: String, partition: Int, transaction: Long): TwitterFuture[Boolean] =
    request.setConsumerState(token,name,stream,partition,transaction)
  override def getConsumerState(token: String, name: String, stream: String, partition: Int): TwitterFuture[Long] =
    request.getConsumerState(token,name,stream,partition)
}

//object ClientTransaction extends App {
// // implicit lazy val context = transactionService.Context(2)
//  val client = new ClientTransaction("ognelis","228",":8080",new ClientAuth(":8081"))
//
//  println(Await.ready(client.putStream(client.token,"1",5, None)))
//
//  val acc: ArrayBuffer[TwitterFuture[Boolean]] = new ArrayBuffer[TwitterFuture[Boolean]]()
//
//  val producerTransactions = (0 to 1000).map(_ => new ProducerTransaction {
//    override val transactionID: Long = scala.util.Random.nextLong()
//
//    override val state: TransactionStates = TransactionStates.Opened
//
//    override val stream: String = "1"
//
//    override val timestamp: Long = Time.epoch.inNanoseconds
//
//    override val quantity: Int = -1
//
//    override val partition: Int = scala.util.Random.nextInt(10000)
//
//    override def tll: Long = Time.epoch.inNanoseconds
//  })
//
//  val consumerTransactions = (0 to 1000).map(_ => new ConsumerTransaction {
//    override def transactionID: Long = scala.util.Random.nextLong()
//
//    override def name: String = scala.util.Random.nextInt(1000).toString
//
//    override def stream: String = "1"
//
//    override def partition: Int = scala.util.Random.nextInt(10000)
//  })
//
//  val transactions = (producerTransactions++consumerTransactions).map{
//    case txn: ProducerTransaction => new Transaction {
//      override def producerTransaction: Option[ProducerTransaction] = Some(txn)
//      override def consumerTransaction: Option[ConsumerTransaction] = None
//    }
//    case txn: ConsumerTransaction => new Transaction {
//      override def producerTransaction: Option[ProducerTransaction] = None
//      override def consumerTransaction: Option[ConsumerTransaction] = Some(txn)
//    }
//  }
//
//  println(Await.ready(client.putTransactions(client.token, transactions)))
//}
