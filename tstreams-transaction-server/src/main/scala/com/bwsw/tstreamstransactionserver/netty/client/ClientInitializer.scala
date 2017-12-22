///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package com.bwsw.tstreamstransactionserver.netty.client
//
//import java.util.concurrent.ConcurrentMap
//
//import com.bwsw.tstreamstransactionserver.netty.{Message, ResponseMessage}
//import io.netty.buffer.ByteBuf
//import io.netty.channel.ChannelInitializer
//import io.netty.channel.socket.SocketChannel
//import io.netty.handler.codec.LengthFieldBasedFrameDecoder
//import io.netty.handler.codec.bytes.ByteArrayEncoder
//
//import scala.concurrent.{Promise => ScalaPromise}
//
//class ClientInitializer(reqIdToRep: ConcurrentMap[Long, ScalaPromise[ByteBuf]])
//  extends ChannelInitializer[SocketChannel] {
//
//  override def initChannel(ch: SocketChannel): Unit = {
//    ch.pipeline()
//      .addLast(new ByteArrayEncoder())
////      .addLast(new LengthFieldBasedFrameDecoder(
////        Int.MaxValue,
////        ResponseMessage.headerFieldSize,
////        ResponseMessage.lengthFieldSize)
////      )
//      .addLast(new ClientHandler(reqIdToRep))
//  }
//}
//
//
//
