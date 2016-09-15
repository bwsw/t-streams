package testutils

import com.bwsw.tstreams.generator.LocalTimeUUIDGenerator

/**
  * Helper object for creating LocalTimeTransactionGenerator
  */
object LocalGeneratorCreator {
  def getGen() = new LocalTimeUUIDGenerator
}