package com.bwsw.tstreams.converter

/**
  * Basic converter string to array byte
  */
class StringToArrayByteConverter extends IConverter[String, Array[Byte]] {
  override def convert(obj: String): Array[Byte] = obj.getBytes
}

/**
  * Basic converter array byte to string
  */
class ArrayByteToStringConverter extends IConverter[Array[Byte], String] {
  override def convert(obj: Array[Byte]): String = new String(obj)
}

