package com.bwsw.tstreamstransactionserver.netty.server.multiNode

import java.util

import com.bwsw.tstreamstransactionserver.rpc.ProducerTransactionsAndData
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec3}
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.transport.{TMemoryBuffer, TMemoryInputTransport}

object Structure {
  private val protocolTCompactFactory = new TCompactProtocol.Factory

  abstract class StructureSerializable[Struct <: ThriftStruct](codec: ThriftStructCodec3[Struct]) {
    @inline
    final def encode(entity: Struct): Array[Byte] = {
      val buffer = new TMemoryBuffer(128)
      val oprot = protocolTCompactFactory
        .getProtocol(buffer)

      codec.encode(entity, oprot)
      util.Arrays.copyOfRange(buffer.getArray, 0, buffer.length)
    }

    @inline
    final def decode(bytes: Array[Byte]): Struct = {
      val iprot = protocolTCompactFactory
        .getProtocol(new TMemoryInputTransport(bytes))

      codec.decode(iprot)
    }
  }

  object PutTransactionsAndData
    extends StructureSerializable(ProducerTransactionsAndData)
}
