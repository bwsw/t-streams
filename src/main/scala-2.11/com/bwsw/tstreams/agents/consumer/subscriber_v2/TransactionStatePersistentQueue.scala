package com.bwsw.tstreams.agents.consumer.subscriber_v2

import java.io.StringWriter

import com.bwsw.tstreams.common.AbstractPersistentQueue
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
class TransactionStatePersistentQueue(basePath: String)
  extends AbstractPersistentQueue[List[TransactionState]](basePath: String) {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  override def serialize(elt: Object): String = {
    val out = new StringWriter
    mapper.writeValue(out, elt.asInstanceOf[List[TransactionState]])
    val r = out.toString()
    r
  }

  override def deserialize(data: String): Object = {
    mapper.readValue(data, new TypeReference[List[TransactionState]] {}).asInstanceOf[Object]
  }

}
