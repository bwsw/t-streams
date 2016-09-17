package com.bwsw.tstreams.common

import java.util.{Comparator}

/**
  * Comparator which compare two ID's
  */
object TransactionComparator extends Comparator[Long] {
  override def compare(elem1: Long, elem2: Long): Int = {
    if (elem1 > elem2) 1
    else if (elem1 < elem2) -1
    else 0
  }
}