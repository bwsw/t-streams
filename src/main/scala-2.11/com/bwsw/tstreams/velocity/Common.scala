package com.bwsw.tstreams.velocity

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import com.aerospike.client.Host
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.streams.BasicStream

object Common {
  implicit val system = ActorSystem("UTEST")
  val keyspace = "velocity"

  //metadata/data factories
  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new AerospikeStorageFactory

  //converters to convert usertype->storagetype; storagetype->usertype
  val stringToArrayByteConverter = new StringToArrayByteConverter
  val arrayByteToStringConverter = new ArrayByteToStringConverter

  //aerospike storage instances
  val hosts = List(
    new Host("t-streams-1.z1.netpoint-dc.com", 3000),
    new Host("t-streams-1.z1.netpoint-dc.com", 3001),
    new Host("t-streams-1.z1.netpoint-dc.com", 3002),
    new Host("t-streams-1.z1.netpoint-dc.com", 3003))
  val aerospikeOptions = new AerospikeStorageOptions("test", hosts)
  val aerospikeInst = storageFactory.getInstance(aerospikeOptions)

  //metadata storage instances
  val metadataStorageInst = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("t-streams-1.z1.netpoint-dc.com", 9042)),
    keyspace = keyspace)

  //stream instances for producer/consumer
  val stream: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 1,
    metadataStorage = metadataStorageInst,
    dataStorage = aerospikeInst,
    ttl = 60 * 15,
    description = "some_description")
}
