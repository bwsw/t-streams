package com.bwsw.tstreamstransactionserver.netty.server.subscriber

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamKey, StreamValue}
import org.scalatest.{FlatSpec, Matchers}
import util.{SubscriberUtils, Utils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SubscribersObserverTest
  extends FlatSpec
    with Matchers
{

  "Subscribers observer" should "throws exception if it is shutdown more than once" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val zookeeperStreamRepository = new ZookeeperStreamRepository(zkClient, "/tts")
    val timeToUpdateMs = 200

    val observer = new SubscribersObserver(
      zkClient,
      zookeeperStreamRepository,
      timeToUpdateMs
    )

    observer.shutdown()
    zkClient.close()
    zkServer.close()

    assertThrows[IllegalStateException](
      observer.shutdown()
    )
  }


  it should "return none subscribers" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val zookeeperStreamRepository = new ZookeeperStreamRepository(zkClient, "/tts")
    val timeToUpdateMs = 200

    val observer = new SubscribersObserver(
      zkClient,
      zookeeperStreamRepository,
      timeToUpdateMs
    )

    val rand = scala.util.Random
    val streamKeys  = new ArrayBuffer[StreamKey]()
    (0 to 10).foreach{index =>
      val streamBody = StreamValue(index.toString, 100, None, 1000L, None)

      streamKeys += zookeeperStreamRepository.put(streamBody)

      observer.addSteamPartition(index, rand.nextInt(100))
    }

    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    streamKeys foreach {key =>
      observer.getStreamPartitionSubscribers(key.id, 1) shouldBe None
    }

    observer.shutdown()
    zkClient.close()
    zkServer.close()
  }

  it should "return all subscribers" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val zookeeperStreamRepository = new ZookeeperStreamRepository(zkClient, "/tts")
    val timeToUpdateMs = 200

    val maxStreams = 10
    val partitionMax = 7
    val subscriberMax = 5

    val observer = new SubscribersObserver(
      zkClient,
      zookeeperStreamRepository,
      timeToUpdateMs
    )

    val rand = scala.util.Random
    val streamKeys  = new ArrayBuffer[StreamKey]()

    val streamPartitions = mutable.Map[Int, ArrayBuffer[Int]]()
    val streamPartitionSubscribersNumber = mutable.Map[(Int, Int), Int]()
    (0 to maxStreams).foreach{streamID =>
      val streamBody = StreamValue(streamID.toString, partitionMax, None, 1000L, None)
      val streamKey  = zookeeperStreamRepository.put(streamBody)
      streamKeys += streamKey

      val streamRecord = zookeeperStreamRepository.get(streamKey)
      val pathToStream = streamRecord.get.zkPath

      (0 to rand.nextInt(partitionMax)) foreach { partition =>
        val subscriberNumber = rand.nextInt(subscriberMax)
        streamPartitionSubscribersNumber.put((streamID, partition), subscriberNumber + 1)
        (0 to subscriberNumber) foreach { subscriber =>
          SubscriberUtils.putSubscriberInStream(zkClient, pathToStream, partition, subscriber.toString)
        }
        streamPartitions.get(streamID) match {
          case None =>
            streamPartitions.put(streamID, ArrayBuffer.empty[Int])
          case Some(acc) =>
            acc += partition
        }
        observer.addSteamPartition(streamID, partition)
      }
    }

    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    streamKeys foreach {key =>
      streamPartitions(key.id).foreach{partition =>
        val subscriberNumber = observer
          .getStreamPartitionSubscribers(key.id, partition)
          .get.size
        subscriberNumber shouldBe streamPartitionSubscribersNumber(key.id, partition)
      }
    }

    observer.shutdown()
    zkClient.close()
    zkServer.close()
  }


  it should "return a subscriber before removing it on zk path and return None after removing the subscriber" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val zookeeperStreamRepository = new ZookeeperStreamRepository(zkClient, "/tts")
    val timeToUpdateMs = 200

    val observer = new SubscribersObserver(
      zkClient,
      zookeeperStreamRepository,
      timeToUpdateMs
    )

    val streamBody = StreamValue(0.toString, 100, None, 1000L, None)
    val streamKey   = zookeeperStreamRepository.put(streamBody)
    val streamRecord = zookeeperStreamRepository.get(streamKey).get
    val partition  = 1

    observer.addSteamPartition(streamKey.id, partition)
    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    observer
      .getStreamPartitionSubscribers(streamKey.id, partition) shouldBe None

    SubscriberUtils.putSubscriberInStream(zkClient, streamRecord.zkPath, partition, "test")
    observer.addSteamPartition(streamKey.id, partition)
    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    observer
      .getStreamPartitionSubscribers(streamKey.id, partition)
      .get.get(0) shouldBe "test"

    SubscriberUtils.deleteSubscriberInStream(zkClient, streamRecord.zkPath, partition, "test")
    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    observer
      .getStreamPartitionSubscribers(streamKey.id, partition) shouldBe None


    observer.shutdown()
    zkClient.close()
    zkServer.close()
  }


}
