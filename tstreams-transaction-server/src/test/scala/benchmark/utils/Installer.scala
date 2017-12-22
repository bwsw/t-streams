package benchmark.utils

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}

import benchmark.Options._
import org.apache.commons.io.FileUtils

import scala.concurrent.Await
import scala.concurrent.duration._


trait Installer {
  private val storageOptions = serverBuilder.getStorageOptions

  def clearDB() = {
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
  }

  def startTransactionServer() = {
    val latch = new CountDownLatch(1)
    new Thread(() =>
      serverBuilder
        .build()
        .start(latch.countDown())
    ).start()
    latch.await(5000, TimeUnit.MILLISECONDS)
  }

  def createStream(name: String, partitions: Int): Int = {
    val client = clientBuilder
      .build()


    val streamID = if (Await.result(client.checkStreamExists(name), 5.seconds)) {
      Await.result(client.getStream(name), 5.seconds).map(_.id).getOrElse(
        throw new IllegalArgumentException("Something wrong with stream")
      )
    } else {
      Await.result(client.putStream(name, partitions, None, 5), 5.seconds)
    }
    client.shutdown()
    streamID
  }

  def deleteStream(name: String) = {
    val client = clientBuilder.build()
    Await.result(client.delStream(name), 10.seconds)

    client.shutdown()
  }
}
