package util

import com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction


object Implicit {

  implicit object ProducerTransactionSortable extends Ordering[ProducerTransaction] {
    override def compare(x: ProducerTransaction, y: ProducerTransaction): Int = {
      if (x.stream > y.stream) 1
      else if (x.stream < y.stream) -1
      else if (x.partition > y.partition) 1
      else if (x.partition < y.partition) -1
      else if (x.transactionID > y.transactionID) 1
      else if (x.transactionID < y.transactionID) -1
      else if (x.state.value < y.state.value) -1
      else if (x.state.value > y.state.value) 1
      else 0
    }
  }

}