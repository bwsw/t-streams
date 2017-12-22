package ut

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates._

class TransactionStateHandlerTestSuite
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  //arrange
  private val transactionStateHandler = ProducerTransactionStateMachine
  private val ts = 640836800000L
  private val openedTTL = 2
  private val quantity = -1
  private val streamID = 101
  private val streamPartitions = 1


  it should "not put producerTransaction with state: Invalid. " +
    "It should return Invalid state, as it's the one transaction." in {
    //arrange
    val producerTransaction = createProducerTransaction(Invalid, ts)

    //act
    val finalState = transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        producerTransaction,
      )).get

    //assert
    finalState.isInstanceOf[InvalidTransactionState] shouldBe true
  }

  it should "not put producerTransaction with state: Cancel. " +
    "It should return Cancel state, as it's the one transaction." in {
    //arrange
    val producerTransaction = createProducerTransaction(Cancel, ts)

    //act
    val finalState = transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        producerTransaction,
      )).get

    //assert
    finalState.isInstanceOf[CanceledTransactionState] shouldBe true
  }

  it should "not put producerTransaction with state: Updated. " +
    "It should return Updated state, as it's the one transaction." in {
    //arrange
    val producerTransaction = createProducerTransaction(Updated, ts)

    //act
    val finalState = transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        producerTransaction,
      )).get

    //assert
    finalState.isInstanceOf[UpdatedTransactionState] shouldBe true
  }

  it should "not process the following chain of states of producer transactions: Opened -> Invalid. " +
    "It should return OpenedTransactionState, as there is no transition from Opened to Invalid state" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val invalidProducerTransaction = createProducerTransaction(Invalid, ts + 1)

    //act
    val finalState = transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        openedProducerTransaction,
        invalidProducerTransaction,
      )).get

    finalState.isInstanceOf[OpenedTransactionState] shouldBe true
  }

  it should "not process the following chain of states of producer transactions: Opened -> Updated -> Updated -> Invalid. " +
    "It should return OpenedTransactionState, as there is no transition from Opened to Checkpointed state" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction1 = createProducerTransaction(Updated, ts + 1)
    val updatedProducerTransaction2 = createProducerTransaction(Updated, ts + 2)
    val invalidProducerTransaction = createProducerTransaction(Invalid, ts + 3)

    //act and assert
    val finalState = transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        openedProducerTransaction,
        updatedProducerTransaction1,
        updatedProducerTransaction2,
        invalidProducerTransaction
      )).get

    finalState.isInstanceOf[OpenedTransactionState] shouldBe true
  }

  it should "process the following chain of states of producer transactions: Opened -> Checkpointed. " +
    "The final state of transaction should be Checkpointed" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 1)

    //act
    val finalState = transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        openedProducerTransaction,
        checkpointedProducerTransaction,
      )).get

    //assert
    finalState.isInstanceOf[CheckpointedTransactionState] shouldBe true
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Updated -> Checkpointed. " +
    "The final state of transaction should be Checkpointed" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction1 = createProducerTransaction(Updated, ts + 1)
    val updatedProducerTransaction2 = createProducerTransaction(Updated, ts + 2)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 3)

    //act
    //act and assert
    val finalState =  transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        openedProducerTransaction,
        updatedProducerTransaction1,
        updatedProducerTransaction2,
        checkpointedProducerTransaction
      )).get

    //assert
    finalState.isInstanceOf[CheckpointedTransactionState] shouldBe true
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Checkpointed -> Cancel. " +
    "The final state of transaction should be Checkpointed, because it should ignore the part of chain following after Checkpointed state." in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction = createProducerTransaction(Updated, ts + 1)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 2)
    val cancelProducerTransaction = createProducerTransaction(Cancel, ts + 3)


    //act
    val finalState =  transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        openedProducerTransaction,
        updatedProducerTransaction,
        checkpointedProducerTransaction,
        cancelProducerTransaction
      )).get

    //assert
    finalState.isInstanceOf[CheckpointedTransactionState] shouldBe true
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Checkpointed -> Invalid. " +
    "The final state of transaction should be Checkpointed, because it should ignore the part of chain following after Checkpointed state." in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction = createProducerTransaction(Updated, ts + 1)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 2)
    val cancelProducerTransaction = createProducerTransaction(Invalid, ts + 3)


    //act
    val finalState =  transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        openedProducerTransaction,
        updatedProducerTransaction,
        checkpointedProducerTransaction,
        cancelProducerTransaction
      )).get

    //assert
    finalState.isInstanceOf[CheckpointedTransactionState] shouldBe true
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Checkpointed -> Opened -> Cancel. " +
    "The final state of transaction should be Checkpointed, because it should ignore the part of chain following after Checkpointed state." in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction = createProducerTransaction(Updated, ts + 1)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 2)
    val openedProducerTransaction2 = createProducerTransaction(Opened, ts + 3)
    val cancelProducerTransaction = createProducerTransaction(Cancel, ts + 4)

    //act
    val finalState =  transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        openedProducerTransaction,
        updatedProducerTransaction,
        checkpointedProducerTransaction,
        openedProducerTransaction2,
        cancelProducerTransaction
      )).get

    //assert
    finalState.isInstanceOf[CheckpointedTransactionState] shouldBe true
  }

  it should "process the following chain of states of producer transactions: Opened -> Cancel. " +
    "The final state of transaction should be Invalid" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val cancelProducerTransaction = createProducerTransaction(Cancel, ts + 1)

    //act
    val finalState =  transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        openedProducerTransaction,
        cancelProducerTransaction
      )).get

    //assert
    finalState.isInstanceOf[InvalidTransactionState] shouldBe true
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Updated -> Cancel." +
    "The final state of transaction should be Invalid" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction1 = createProducerTransaction(Updated, ts + 1)
    val updatedProducerTransaction2 = createProducerTransaction(Updated, ts + 2)
    val cancelProducerTransaction = createProducerTransaction(Cancel, ts + 3)


    //act
    val finalState =  transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        openedProducerTransaction,
        updatedProducerTransaction1,
        updatedProducerTransaction2,
        cancelProducerTransaction
      )).get

    //assert
    finalState.isInstanceOf[InvalidTransactionState] shouldBe true
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Cancel -> Checkpointed. " +
    "The final state of transaction should be Invalid because it should ignore the part of chain following after Cancel state" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction1 = createProducerTransaction(Updated, ts + 1)
    val cancelProducerTransaction = createProducerTransaction(Cancel, ts + 2)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 3)

    //act
    val finalState =  transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        openedProducerTransaction,
        updatedProducerTransaction1,
        cancelProducerTransaction,
        checkpointedProducerTransaction
      )).get

    //assert
    finalState.isInstanceOf[InvalidTransactionState] shouldBe true
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Cancel -> Opened -> Checkpointed. " +
    "The final state of transaction should be Invalid  because it should ignore the part of chain following after Cancel state" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction1 = createProducerTransaction(Updated, ts + 1)
    val cancelProducerTransaction = createProducerTransaction(Cancel, ts + 2)
    val openedProducerTransaction2 = createProducerTransaction(Opened, ts + 3)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 4)

    //act
    val finalState =  transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        openedProducerTransaction,
        updatedProducerTransaction1,
        cancelProducerTransaction,
        openedProducerTransaction2,
        checkpointedProducerTransaction
      )).get

    //assert
    finalState.isInstanceOf[InvalidTransactionState] shouldBe true
  }

  it should "process the case in which a producer transaction remains in Opened state" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)


    //act
    val finalState =  transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        openedProducerTransaction,
      )).get

    //assert
    finalState.isInstanceOf[OpenedTransactionState] shouldBe true
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Updated. " +
    "The final state of transaction should be Opened" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction1 = createProducerTransaction(Updated, ts + 1)
    val updatedProducerTransaction2 = createProducerTransaction(Updated, ts + 2)

    //act
    val finalState =  transactionStateHandler.transiteTransactionsToFinalState(
      Seq(
        openedProducerTransaction,
        updatedProducerTransaction1,
        updatedProducerTransaction2
      )).get

    //assert
    finalState.isInstanceOf[OpenedTransactionState] shouldBe true
  }

  private def createProducerTransaction(transactionState: TransactionStates, ts: Long) = {
    val producerTransaction =
      com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
        streamID,
        streamPartitions,
        ts,
        transactionState,
        quantity,
        openedTTL
      )
    ProducerTransactionRecord(producerTransaction, ts)
  }
}