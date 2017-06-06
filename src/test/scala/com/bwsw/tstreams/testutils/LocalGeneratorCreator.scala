package com.bwsw.tstreams.testutils

import com.bwsw.tstreams.generator.TransactionGenerator

/**
  * Helper object for creating LocalTimeTransactionGenerator
  */
object LocalGeneratorCreator {
  val gen = new TransactionGenerator

  def getGen() = gen

  def getTransaction() = gen.getTransaction()
}