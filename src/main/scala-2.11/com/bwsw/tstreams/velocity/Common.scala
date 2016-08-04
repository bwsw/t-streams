package com.bwsw.tstreams.velocity

import java.net.InetSocketAddress

import com.aerospike.client.Host
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.streams.TStream

object Common {
  val zkHost = "176.120.27.82"
  val keyspace = "velocity"

  //metadata/data factories
  lazy val metadataStorageFactory = new MetadataStorageFactory
  lazy val storageFactory = new AerospikeStorageFactory

  //converters to convert usertype->storagetype; storagetype->usertype
  val stringToArrayByteConverter = new StringToArrayByteConverter
  val arrayByteToStringConverter = new ArrayByteToStringConverter

  //aerospike storage instances
  lazy val hosts = List(
    new Host("176.120.27.82", 3000),
    new Host("176.120.27.82", 3001),
    new Host("176.120.27.82", 3002),
    new Host("176.120.27.82", 3003))
  lazy val aerospikeOptions = new AerospikeStorageOptions("test", hosts)
  lazy val aerospikeInst = storageFactory.getInstance(aerospikeOptions)

  //metadata storage instances
  lazy val metadataStorageInst = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("176.120.27.82", 9042)),
    keyspace = keyspace)

  //stream instances for producer/consumer
  lazy val stream: TStream[Array[Byte]] = new TStream[Array[Byte]](
    name = "test_stream",
    partitions = 1,
    metadataStorage = metadataStorageInst,
    dataStorage = aerospikeInst,
    ttl = 60 * 15,
    description = "some_description")
}
