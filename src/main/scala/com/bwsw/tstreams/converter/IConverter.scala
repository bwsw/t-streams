package com.bwsw.tstreams.converter

/**
  * Converter to convert IN type into OUT type
  *
  * @tparam IN  type
  * @tparam OUT type
  */
trait IConverter[IN, OUT] {
  def convert(obj: IN): OUT
}
