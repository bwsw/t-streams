package com.bwsw.tstreams.transactionServer

import com.bwsw.tstreams.transactionServer.rpc.TransactionServerService.ScanTransactionsCRC32
import com.twitter.finagle.Thrift
import com.twitter.finagle.service.RetryPolicy
import com.twitter.util.{Await, Try}


/**
  * Created by Ivan Kudryavtsev on 22.10.16.
  */
object TransactionClient {
  def main(args: Array[String]) {

    val retryPolicy = RetryPolicy.tries[Try[ScanTransactionsCRC32.Result]](3,
      {
        case res => {
          true
        }
      })

    val client = Thrift.client.newIface[rpc.TransactionServerService.FutureIface]("localhost:8888")
    val start = System.currentTimeMillis()
    println(Await.result(client.scanTransactionsCRC32("","",0,0)))
    val end = System.currentTimeMillis()
    println(end - start)
    //#thriftclientapi
  }
}
