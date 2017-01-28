package metadata

import org.scalatest._
import testutils.TestUtils


class MetadataStorageTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val connectedSession = cluster.connect(randomKeyspace)

  "MetadataStorage.init(), MetadataStorage.truncate() and MetadataStorage.remove()" should "create, truncate and remove metadata tables" in {
    val metadataStorage = new MetadataStorage(cluster, connectedSession, randomKeyspace)
    var checkVal = true

    try {
      //here we already have created tables for entitiesRepository
      metadataStorage.remove()
      metadataStorage.init()
      metadataStorage.truncate()
    }
    catch {
      case e: Exception => checkVal = false
    }
    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}