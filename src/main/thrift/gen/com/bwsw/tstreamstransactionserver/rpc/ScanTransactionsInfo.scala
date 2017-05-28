/**
 * Generated by Scrooge
 *   version: 4.16.0
 *   rev: 0201cac9fdd6188248d42da91fd14c87744cc4a5
 *   built at: 20170421-124523
 */
package com.bwsw.tstreamstransactionserver.rpc

import com.twitter.scrooge.{
  HasThriftStructCodec3,
  LazyTProtocol,
  TFieldBlob,
  ThriftException,
  ThriftStruct,
  ThriftStructCodec3,
  ThriftStructFieldInfo,
  ThriftStructMetaData,
  ThriftUtil
}
import org.apache.thrift.protocol._
import org.apache.thrift.transport.{TMemoryBuffer, TTransport}
import java.nio.ByteBuffer
import java.util.Arrays
import scala.collection.immutable.{Map => immutable$Map}
import scala.collection.mutable.Builder
import scala.collection.mutable.{
  ArrayBuffer => mutable$ArrayBuffer, Buffer => mutable$Buffer,
  HashMap => mutable$HashMap, HashSet => mutable$HashSet}
import scala.collection.{Map, Set}


object ScanTransactionsInfo extends ThriftStructCodec3[ScanTransactionsInfo] {
  private val NoPassthroughFields = immutable$Map.empty[Short, TFieldBlob]
  val Struct = new TStruct("ScanTransactionsInfo")
  val LastOpenedTransactionIDField = new TField("lastOpenedTransactionID", TType.I64, 1)
  val LastOpenedTransactionIDFieldManifest = implicitly[Manifest[Long]]
  val ProducerTransactionsField = new TField("producerTransactions", TType.LIST, 2)
  val ProducerTransactionsFieldManifest = implicitly[Manifest[Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]]]

  /**
   * Field information in declaration order.
   */
  lazy val fieldInfos: scala.List[ThriftStructFieldInfo] = scala.List[ThriftStructFieldInfo](
    new ThriftStructFieldInfo(
      LastOpenedTransactionIDField,
      false,
      true,
      LastOpenedTransactionIDFieldManifest,
      _root_.scala.None,
      _root_.scala.None,
      immutable$Map.empty[String, String],
      immutable$Map.empty[String, String],
      None
    ),
    new ThriftStructFieldInfo(
      ProducerTransactionsField,
      false,
      true,
      ProducerTransactionsFieldManifest,
      _root_.scala.None,
      _root_.scala.Some(implicitly[Manifest[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]]),
      immutable$Map.empty[String, String],
      immutable$Map.empty[String, String],
      None
    )
  )

  lazy val structAnnotations: immutable$Map[String, String] =
    immutable$Map.empty[String, String]

  /**
   * Checks that all required fields are non-null.
   */
  def validate(_item: ScanTransactionsInfo): Unit = {
    if (_item.producerTransactions == null) throw new TProtocolException("Required field producerTransactions cannot be null")
  }

  def withoutPassthroughFields(original: ScanTransactionsInfo): ScanTransactionsInfo =
    new Immutable(
      lastOpenedTransactionID =
        {
          val field = original.lastOpenedTransactionID
          field
        },
      producerTransactions =
        {
          val field = original.producerTransactions
          field.map { field =>
            com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction.withoutPassthroughFields(field)
          }
        }
    )

  override def encode(_item: ScanTransactionsInfo, _oproto: TProtocol): Unit = {
    _item.write(_oproto)
  }

  private[this] def lazyDecode(_iprot: LazyTProtocol): ScanTransactionsInfo = {

    var lastOpenedTransactionID: Long = 0L
    var _got_lastOpenedTransactionID = false
    var producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction] = Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]()
    var _got_producerTransactions = false

    var _passthroughFields: Builder[(Short, TFieldBlob), immutable$Map[Short, TFieldBlob]] = null
    var _done = false
    val _start_offset = _iprot.offset

    _iprot.readStructBegin()
    while (!_done) {
      val _field = _iprot.readFieldBegin()
      if (_field.`type` == TType.STOP) {
        _done = true
      } else {
        _field.id match {
          case 1 =>
            _field.`type` match {
              case TType.I64 =>
    
                lastOpenedTransactionID = readLastOpenedTransactionIDValue(_iprot)
                _got_lastOpenedTransactionID = true
              case _actualType =>
                val _expectedType = TType.I64
                throw new TProtocolException(
                  "Received wrong type for field 'lastOpenedTransactionID' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 2 =>
            _field.`type` match {
              case TType.LIST =>
    
                producerTransactions = readProducerTransactionsValue(_iprot)
                _got_producerTransactions = true
              case _actualType =>
                val _expectedType = TType.LIST
                throw new TProtocolException(
                  "Received wrong type for field 'producerTransactions' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case _ =>
            if (_passthroughFields == null)
              _passthroughFields = immutable$Map.newBuilder[Short, TFieldBlob]
            _passthroughFields += (_field.id -> TFieldBlob.read(_field, _iprot))
        }
        _iprot.readFieldEnd()
      }
    }
    _iprot.readStructEnd()

    if (!_got_lastOpenedTransactionID) throw new TProtocolException("Required field 'lastOpenedTransactionID' was not found in serialized data for struct ScanTransactionsInfo")
    if (!_got_producerTransactions) throw new TProtocolException("Required field 'producerTransactions' was not found in serialized data for struct ScanTransactionsInfo")
    new LazyImmutable(
      _iprot,
      _iprot.buffer,
      _start_offset,
      _iprot.offset,
      lastOpenedTransactionID,
      producerTransactions,
      if (_passthroughFields == null)
        NoPassthroughFields
      else
        _passthroughFields.result()
    )
  }

  override def decode(_iprot: TProtocol): ScanTransactionsInfo =
    _iprot match {
      case i: LazyTProtocol => lazyDecode(i)
      case i => eagerDecode(i)
    }

  private[this] def eagerDecode(_iprot: TProtocol): ScanTransactionsInfo = {
    var lastOpenedTransactionID: Long = 0L
    var _got_lastOpenedTransactionID = false
    var producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction] = Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]()
    var _got_producerTransactions = false
    var _passthroughFields: Builder[(Short, TFieldBlob), immutable$Map[Short, TFieldBlob]] = null
    var _done = false

    _iprot.readStructBegin()
    while (!_done) {
      val _field = _iprot.readFieldBegin()
      if (_field.`type` == TType.STOP) {
        _done = true
      } else {
        _field.id match {
          case 1 =>
            _field.`type` match {
              case TType.I64 =>
                lastOpenedTransactionID = readLastOpenedTransactionIDValue(_iprot)
                _got_lastOpenedTransactionID = true
              case _actualType =>
                val _expectedType = TType.I64
                throw new TProtocolException(
                  "Received wrong type for field 'lastOpenedTransactionID' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 2 =>
            _field.`type` match {
              case TType.LIST =>
                producerTransactions = readProducerTransactionsValue(_iprot)
                _got_producerTransactions = true
              case _actualType =>
                val _expectedType = TType.LIST
                throw new TProtocolException(
                  "Received wrong type for field 'producerTransactions' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case _ =>
            if (_passthroughFields == null)
              _passthroughFields = immutable$Map.newBuilder[Short, TFieldBlob]
            _passthroughFields += (_field.id -> TFieldBlob.read(_field, _iprot))
        }
        _iprot.readFieldEnd()
      }
    }
    _iprot.readStructEnd()

    if (!_got_lastOpenedTransactionID) throw new TProtocolException("Required field 'lastOpenedTransactionID' was not found in serialized data for struct ScanTransactionsInfo")
    if (!_got_producerTransactions) throw new TProtocolException("Required field 'producerTransactions' was not found in serialized data for struct ScanTransactionsInfo")
    new Immutable(
      lastOpenedTransactionID,
      producerTransactions,
      if (_passthroughFields == null)
        NoPassthroughFields
      else
        _passthroughFields.result()
    )
  }

  def apply(
    lastOpenedTransactionID: Long,
    producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction] = Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]()
  ): ScanTransactionsInfo =
    new Immutable(
      lastOpenedTransactionID,
      producerTransactions
    )

  def unapply(_item: ScanTransactionsInfo): _root_.scala.Option[_root_.scala.Tuple2[Long, Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]]] = _root_.scala.Some(_item.toTuple)


  @inline private def readLastOpenedTransactionIDValue(_iprot: TProtocol): Long = {
    _iprot.readI64()
  }

  @inline private def writeLastOpenedTransactionIDField(lastOpenedTransactionID_item: Long, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(LastOpenedTransactionIDField)
    writeLastOpenedTransactionIDValue(lastOpenedTransactionID_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeLastOpenedTransactionIDValue(lastOpenedTransactionID_item: Long, _oprot: TProtocol): Unit = {
    _oprot.writeI64(lastOpenedTransactionID_item)
  }

  @inline private def readProducerTransactionsValue(_iprot: TProtocol): Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction] = {
    val _list = _iprot.readListBegin()
    if (_list.size == 0) {
      _iprot.readListEnd()
      Nil
    } else {
      val _rv = new mutable$ArrayBuffer[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction](_list.size)
      var _i = 0
      while (_i < _list.size) {
        _rv += {
          com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction.decode(_iprot)
        }
        _i += 1
      }
      _iprot.readListEnd()
      _rv
    }
  }

  @inline private def writeProducerTransactionsField(producerTransactions_item: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction], _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(ProducerTransactionsField)
    writeProducerTransactionsValue(producerTransactions_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeProducerTransactionsValue(producerTransactions_item: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction], _oprot: TProtocol): Unit = {
    _oprot.writeListBegin(new TList(TType.STRUCT, producerTransactions_item.size))
    producerTransactions_item match {
      case _: IndexedSeq[_] =>
        var _i = 0
        val _size = producerTransactions_item.size
        while (_i < _size) {
          val producerTransactions_item_element = producerTransactions_item(_i)
          producerTransactions_item_element.write(_oprot)
          _i += 1
        }
      case _ =>
        producerTransactions_item.foreach { producerTransactions_item_element =>
          producerTransactions_item_element.write(_oprot)
        }
    }
    _oprot.writeListEnd()
  }


  object Immutable extends ThriftStructCodec3[ScanTransactionsInfo] {
    override def encode(_item: ScanTransactionsInfo, _oproto: TProtocol): Unit = { _item.write(_oproto) }
    override def decode(_iprot: TProtocol): ScanTransactionsInfo = ScanTransactionsInfo.decode(_iprot)
    override lazy val metaData: ThriftStructMetaData[ScanTransactionsInfo] = ScanTransactionsInfo.metaData
  }

  /**
   * The default read-only implementation of ScanTransactionsInfo.  You typically should not need to
   * directly reference this class; instead, use the ScanTransactionsInfo.apply method to construct
   * new instances.
   */
  class Immutable(
      val lastOpenedTransactionID: Long,
      val producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction],
      override val _passthroughFields: immutable$Map[Short, TFieldBlob])
    extends ScanTransactionsInfo {
    def this(
      lastOpenedTransactionID: Long,
      producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction] = Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]()
    ) = this(
      lastOpenedTransactionID,
      producerTransactions,
      Map.empty
    )
  }

  /**
   * This is another Immutable, this however keeps strings as lazy values that are lazily decoded from the backing
   * array byte on read.
   */
  private[this] class LazyImmutable(
      _proto: LazyTProtocol,
      _buf: Array[Byte],
      _start_offset: Int,
      _end_offset: Int,
      val lastOpenedTransactionID: Long,
      val producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction],
      override val _passthroughFields: immutable$Map[Short, TFieldBlob])
    extends ScanTransactionsInfo {

    override def write(_oprot: TProtocol): Unit = {
      _oprot match {
        case i: LazyTProtocol => i.writeRaw(_buf, _start_offset, _end_offset - _start_offset)
        case _ => super.write(_oprot)
      }
    }


    /**
     * Override the super hash code to make it a lazy val rather than def.
     *
     * Calculating the hash code can be expensive, caching it where possible
     * can provide significant performance wins. (Key in a hash map for instance)
     * Usually not safe since the normal constructor will accept a mutable map or
     * set as an arg
     * Here however we control how the class is generated from serialized data.
     * With the class private and the contract that we throw away our mutable references
     * having the hash code lazy here is safe.
     */
    override lazy val hashCode = super.hashCode
  }

  /**
   * This Proxy trait allows you to extend the ScanTransactionsInfo trait with additional state or
   * behavior and implement the read-only methods from ScanTransactionsInfo using an underlying
   * instance.
   */
  trait Proxy extends ScanTransactionsInfo {
    protected def _underlying_ScanTransactionsInfo: ScanTransactionsInfo
    override def lastOpenedTransactionID: Long = _underlying_ScanTransactionsInfo.lastOpenedTransactionID
    override def producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction] = _underlying_ScanTransactionsInfo.producerTransactions
    override def _passthroughFields = _underlying_ScanTransactionsInfo._passthroughFields
  }
}

trait ScanTransactionsInfo
  extends ThriftStruct
  with _root_.scala.Product2[Long, Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]]
  with HasThriftStructCodec3[ScanTransactionsInfo]
  with java.io.Serializable
{
  import ScanTransactionsInfo._

  def lastOpenedTransactionID: Long
  def producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]

  def _passthroughFields: immutable$Map[Short, TFieldBlob] = immutable$Map.empty

  def _1 = lastOpenedTransactionID
  def _2 = producerTransactions

  def toTuple: _root_.scala.Tuple2[Long, Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]] = {
    (
      lastOpenedTransactionID,
      producerTransactions
    )
  }


  /**
   * Gets a field value encoded as a binary blob using TCompactProtocol.  If the specified field
   * is present in the passthrough map, that value is returned.  Otherwise, if the specified field
   * is known and not optional and set to None, then the field is serialized and returned.
   */
  def getFieldBlob(_fieldId: Short): _root_.scala.Option[TFieldBlob] = {
    lazy val _buff = new TMemoryBuffer(32)
    lazy val _oprot = new TCompactProtocol(_buff)
    _passthroughFields.get(_fieldId) match {
      case blob: _root_.scala.Some[TFieldBlob] => blob
      case _root_.scala.None => {
        val _fieldOpt: _root_.scala.Option[TField] =
          _fieldId match {
            case 1 =>
              if (true) {
                writeLastOpenedTransactionIDValue(lastOpenedTransactionID, _oprot)
                _root_.scala.Some(ScanTransactionsInfo.LastOpenedTransactionIDField)
              } else {
                _root_.scala.None
              }
            case 2 =>
              if (producerTransactions ne null) {
                writeProducerTransactionsValue(producerTransactions, _oprot)
                _root_.scala.Some(ScanTransactionsInfo.ProducerTransactionsField)
              } else {
                _root_.scala.None
              }
            case _ => _root_.scala.None
          }
        _fieldOpt match {
          case _root_.scala.Some(_field) =>
            val _data = Arrays.copyOfRange(_buff.getArray, 0, _buff.length)
            _root_.scala.Some(TFieldBlob(_field, _data))
          case _root_.scala.None =>
            _root_.scala.None
        }
      }
    }
  }

  /**
   * Collects TCompactProtocol-encoded field values according to `getFieldBlob` into a map.
   */
  def getFieldBlobs(ids: TraversableOnce[Short]): immutable$Map[Short, TFieldBlob] =
    (ids flatMap { id => getFieldBlob(id) map { id -> _ } }).toMap

  /**
   * Sets a field using a TCompactProtocol-encoded binary blob.  If the field is a known
   * field, the blob is decoded and the field is set to the decoded value.  If the field
   * is unknown and passthrough fields are enabled, then the blob will be stored in
   * _passthroughFields.
   */
  def setField(_blob: TFieldBlob): ScanTransactionsInfo = {
    var lastOpenedTransactionID: Long = this.lastOpenedTransactionID
    var producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction] = this.producerTransactions
    var _passthroughFields = this._passthroughFields
    _blob.id match {
      case 1 =>
        lastOpenedTransactionID = readLastOpenedTransactionIDValue(_blob.read)
      case 2 =>
        producerTransactions = readProducerTransactionsValue(_blob.read)
      case _ => _passthroughFields += (_blob.id -> _blob)
    }
    new Immutable(
      lastOpenedTransactionID,
      producerTransactions,
      _passthroughFields
    )
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is removed
   * from the passthroughFields map, if present.
   */
  def unsetField(_fieldId: Short): ScanTransactionsInfo = {
    var lastOpenedTransactionID: Long = this.lastOpenedTransactionID
    var producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction] = this.producerTransactions

    _fieldId match {
      case 1 =>
        lastOpenedTransactionID = 0L
      case 2 =>
        producerTransactions = Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction]()
      case _ =>
    }
    new Immutable(
      lastOpenedTransactionID,
      producerTransactions,
      _passthroughFields - _fieldId
    )
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is removed
   * from the passthroughFields map, if present.
   */
  def unsetLastOpenedTransactionID: ScanTransactionsInfo = unsetField(1)

  def unsetProducerTransactions: ScanTransactionsInfo = unsetField(2)


  override def write(_oprot: TProtocol): Unit = {
    ScanTransactionsInfo.validate(this)
    _oprot.writeStructBegin(Struct)
    writeLastOpenedTransactionIDField(lastOpenedTransactionID, _oprot)
    if (producerTransactions ne null) writeProducerTransactionsField(producerTransactions, _oprot)
    if (_passthroughFields.nonEmpty) {
      _passthroughFields.values.foreach { _.write(_oprot) }
    }
    _oprot.writeFieldStop()
    _oprot.writeStructEnd()
  }

  def copy(
    lastOpenedTransactionID: Long = this.lastOpenedTransactionID,
    producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction] = this.producerTransactions,
    _passthroughFields: immutable$Map[Short, TFieldBlob] = this._passthroughFields
  ): ScanTransactionsInfo =
    new Immutable(
      lastOpenedTransactionID,
      producerTransactions,
      _passthroughFields
    )

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ScanTransactionsInfo]

  private def _equals(x: ScanTransactionsInfo, y: ScanTransactionsInfo): Boolean =
      x.productArity == y.productArity &&
      x.productIterator.sameElements(y.productIterator)

  override def equals(other: Any): Boolean =
    canEqual(other) &&
      _equals(this, other.asInstanceOf[ScanTransactionsInfo]) &&
      _passthroughFields == other.asInstanceOf[ScanTransactionsInfo]._passthroughFields

  override def hashCode: Int = _root_.scala.runtime.ScalaRunTime._hashCode(this)

  override def toString: String = _root_.scala.runtime.ScalaRunTime._toString(this)


  override def productArity: Int = 2

  override def productElement(n: Int): Any = n match {
    case 0 => this.lastOpenedTransactionID
    case 1 => this.producerTransactions
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productPrefix: String = "ScanTransactionsInfo"

  def _codec: ThriftStructCodec3[ScanTransactionsInfo] = ScanTransactionsInfo
}