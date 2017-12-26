/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
