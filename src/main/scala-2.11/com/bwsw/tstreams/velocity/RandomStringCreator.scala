package com.bwsw.tstreams.velocity

/**
  * Random string generator
  */
object RandomStringCreator {
  private val random = new scala.util.Random

  /**
    * Random string creator
    *
    * @param alphabet base string for creating random string
    * @param n        length of random string
    * @return random string
    */
  private def randomString(alphabet: String)(n: Int): String =
    Stream.continually(random.nextInt(alphabet.length)).map(alphabet).take(n).mkString

  /**
    * Wrapper for random string creation
    *
    * @param n length of string
    * @return random string
    */
  def randomAlphaString(n: Int) = randomString("abcdefghijklmnopqrstuvwxyz")(n)
}
