package benchmark.utils

trait Launcher extends Installer {
  protected val streamName: String
  protected val clients: Int

  def launch() = {
    clearDB()
    startTransactionServer()
    createStream(streamName, clients)
    launchClients()
    deleteStream(streamName)
  }

  protected def launchClients(): Unit
}
