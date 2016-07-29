package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.{Comparator, UUID}

/**
  * Comparator which compare two uuid's
  * uuid with greater timestamp will be greater than the second one
  */
class UUIDComparator extends Comparator[UUID] {
  override def compare(elem1: UUID, elem2: UUID): Int = {
    val ts1 = elem1.timestamp()
    val ts2 = elem2.timestamp()
    if (ts1 > ts2) 1
    else if (ts1 < ts2) -1
    else 0
  }
}