package util


import java.net.{DatagramPacket, DatagramSocket, InetAddress}

final class UdpServer {
  private val port = Utils.getRandomPort
  private val socket: DatagramSocket = {
    new DatagramSocket(port)
  }

  def recieve(timeout: Int): Array[Byte] = {
    if (timeout <= 0) {
      socket.setSoTimeout(0)
    }
    else {
      socket.setSoTimeout(timeout)
    }

    val receiveData: Array[Byte] = new Array[Byte](1024)
    val receivePacket = new DatagramPacket(receiveData, receiveData.length)
    socket.receive(receivePacket)

    receiveData.take(receivePacket.getLength)
  }

  def getPort: Int = port

  def getAddress: InetAddress = socket.getLocalAddress

  def getSocketAddress: String = s"${getAddress.getHostAddress}:$getPort"

  def close() = socket.close()
}
