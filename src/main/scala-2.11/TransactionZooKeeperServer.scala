import authService.AuthClient
import com.sleepycat.je.{CursorConfig, WriteOptions}
import com.twitter.finagle.Thrift
import com.twitter.logging.Level
import transactionService.server.TransactionServer
import com.twitter.util.Await
import com.twitter.util.{Future => TwitterFuture}
import org.apache.curator.retry.ExponentialBackoffRetry
import transactionService.rpc.TransactionStates
import transactionService.server.transactionMetaService.ProducerTransaction
import zooKeeper.ZKLeaderServer


class TransactionZooKeeperServer
  extends TransactionServer({
    import configProperties.ServerConfig._
    new AuthClient(authAddress,authTimeoutConnection,authTimeoutExponentialBetweenRetries)
  }, configProperties.ServerConfig.transactionDataTtlAdd) {

  import configProperties.ServerConfig._

  val zk = new ZKLeaderServer(zkEndpoints,zkTimeoutSession,zkTimeoutConnection,
    new ExponentialBackoffRetry(zkTimeoutBetweenRetries,zkRetriesMax),zkPrefix)

  zk.putData(transactionServerAddress.getBytes())

  private val server = Thrift.server
  def serve = server.serveIface(transactionServerAddress, this)


  private def transiteTxnsToInvalidState() =  {
    import transactionService.server.transactionMetaService.TransactionMetaServiceImpl._
    val transactionDB = environment.beginTransaction(null, null)

    val entities = producerSecondaryIndex.subIndex(TransactionStates.Opened.getValue()).entities(transactionDB, new CursorConfig())
    var txn = entities.next()
    while (txn != null) {
      logger.log(Level.INFO, s"${txn.toString} transit it's state to Invalid!")
      val newInvalidTxn = new ProducerTransaction(txn.transactionID, TransactionStates.Invalid, txn.stream, txn.timestamp, txn.quantity, txn.partition)
      producerPrimaryIndex.put(transactionDB, newInvalidTxn)
      txn = entities.next()
    }

    entities.close()
    transactionDB.commit()
  }

  transiteTxnsToInvalidState()
}

object TransactionZooKeeperServer extends App {
  val server = new TransactionZooKeeperServer

  Await.ready(server.serve)
}
