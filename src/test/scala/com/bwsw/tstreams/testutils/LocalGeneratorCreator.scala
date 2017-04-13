package com.bwsw.tstreams.testutils

import com.bwsw.tstreams.generator.LocalTransactionGenerator

/**
  * Helper object for creating LocalTimeTransactionGenerator
  */
object LocalGeneratorCreator {
  val gen = new LocalTransactionGenerator

  def getGen() = gen

  def getTransaction() = gen.getTransaction()
}