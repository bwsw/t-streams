package benchmark.utils

import java.io.File
import java.util.logging.LogManager

import org.apache.commons.io.FileUtils

import scala.concurrent.Await
import scala.concurrent.duration._


trait Installer {
  private val configServer = new configProperties.ServerConfig(new configProperties.ConfigFile("src/main/resources/serverProperties.properties"))

  def clearDB() = {
    FileUtils.deleteDirectory(new File(configServer.dbPath + "/" + configServer.dbStreamDirName))
    FileUtils.deleteDirectory(new File(configServer.dbPath + "/" + configServer.dbTransactionDataDirName))
    FileUtils.deleteDirectory(new File(configServer.dbPath + "/" + configServer.dbTransactionMetaDirName))
  }

  def startTransactionServer() = {
    new Thread(new Runnable {
      LogManager.getLogManager.reset()

      override def run(): Unit = new netty.server.Server().start()
    }).start()
  }

  def createStream(name: String, partitions: Int) = {
    val client = new netty.client.Client
    Await.result(client.putStream(name, partitions, None, 5), 10.seconds)
  }

  def deleteStream(name: String) = {
    val client = new netty.client.Client
    Await.result(client.delStream(name), 10.seconds)
  }
}
