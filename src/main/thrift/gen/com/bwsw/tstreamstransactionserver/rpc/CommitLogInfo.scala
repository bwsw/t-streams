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


object CommitLogInfo extends ThriftStructCodec3[CommitLogInfo] {
  private val NoPassthroughFields = immutable$Map.empty[Short, TFieldBlob]
  val Struct = new TStruct("CommitLogInfo")
  val CurrentProcessedCommitLogField = new TField("currentProcessedCommitLog", TType.I64, 1)
  val CurrentProcessedCommitLogFieldManifest = implicitly[Manifest[Long]]
  val CurrentConstructedCommitLogField = new TField("currentConstructedCommitLog", TType.I64, 2)
  val CurrentConstructedCommitLogFieldManifest = implicitly[Manifest[Long]]

  /**
   * Field information in declaration order.
   */
  lazy val fieldInfos: scala.List[ThriftStructFieldInfo] = scala.List[ThriftStructFieldInfo](
    new ThriftStructFieldInfo(
      CurrentProcessedCommitLogField,
      false,
      true,
      CurrentProcessedCommitLogFieldManifest,
      _root_.scala.None,
      _root_.scala.None,
      immutable$Map.empty[String, String],
      immutable$Map.empty[String, String],
      None
    ),
    new ThriftStructFieldInfo(
      CurrentConstructedCommitLogField,
      false,
      true,
      CurrentConstructedCommitLogFieldManifest,
      _root_.scala.None,
      _root_.scala.None,
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
  def validate(_item: CommitLogInfo): Unit = {
  }

  def withoutPassthroughFields(original: CommitLogInfo): CommitLogInfo =
    new Immutable(
      currentProcessedCommitLog =
        {
          val field = original.currentProcessedCommitLog
          field
        },
      currentConstructedCommitLog =
        {
          val field = original.currentConstructedCommitLog
          field
        }
    )

  override def encode(_item: CommitLogInfo, _oproto: TProtocol): Unit = {
    _item.write(_oproto)
  }

  private[this] def lazyDecode(_iprot: LazyTProtocol): CommitLogInfo = {

    var currentProcessedCommitLog: Long = 0L
    var _got_currentProcessedCommitLog = false
    var currentConstructedCommitLog: Long = 0L
    var _got_currentConstructedCommitLog = false

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
    
                currentProcessedCommitLog = readCurrentProcessedCommitLogValue(_iprot)
                _got_currentProcessedCommitLog = true
              case _actualType =>
                val _expectedType = TType.I64
                throw new TProtocolException(
                  "Received wrong type for field 'currentProcessedCommitLog' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 2 =>
            _field.`type` match {
              case TType.I64 =>
    
                currentConstructedCommitLog = readCurrentConstructedCommitLogValue(_iprot)
                _got_currentConstructedCommitLog = true
              case _actualType =>
                val _expectedType = TType.I64
                throw new TProtocolException(
                  "Received wrong type for field 'currentConstructedCommitLog' (expected=%s, actual=%s).".format(
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

    if (!_got_currentProcessedCommitLog) throw new TProtocolException("Required field 'currentProcessedCommitLog' was not found in serialized data for struct CommitLogInfo")
    if (!_got_currentConstructedCommitLog) throw new TProtocolException("Required field 'currentConstructedCommitLog' was not found in serialized data for struct CommitLogInfo")
    new LazyImmutable(
      _iprot,
      _iprot.buffer,
      _start_offset,
      _iprot.offset,
      currentProcessedCommitLog,
      currentConstructedCommitLog,
      if (_passthroughFields == null)
        NoPassthroughFields
      else
        _passthroughFields.result()
    )
  }

  override def decode(_iprot: TProtocol): CommitLogInfo =
    _iprot match {
      case i: LazyTProtocol => lazyDecode(i)
      case i => eagerDecode(i)
    }

  private[this] def eagerDecode(_iprot: TProtocol): CommitLogInfo = {
    var currentProcessedCommitLog: Long = 0L
    var _got_currentProcessedCommitLog = false
    var currentConstructedCommitLog: Long = 0L
    var _got_currentConstructedCommitLog = false
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
                currentProcessedCommitLog = readCurrentProcessedCommitLogValue(_iprot)
                _got_currentProcessedCommitLog = true
              case _actualType =>
                val _expectedType = TType.I64
                throw new TProtocolException(
                  "Received wrong type for field 'currentProcessedCommitLog' (expected=%s, actual=%s).".format(
                    ttypeToString(_expectedType),
                    ttypeToString(_actualType)
                  )
                )
            }
          case 2 =>
            _field.`type` match {
              case TType.I64 =>
                currentConstructedCommitLog = readCurrentConstructedCommitLogValue(_iprot)
                _got_currentConstructedCommitLog = true
              case _actualType =>
                val _expectedType = TType.I64
                throw new TProtocolException(
                  "Received wrong type for field 'currentConstructedCommitLog' (expected=%s, actual=%s).".format(
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

    if (!_got_currentProcessedCommitLog) throw new TProtocolException("Required field 'currentProcessedCommitLog' was not found in serialized data for struct CommitLogInfo")
    if (!_got_currentConstructedCommitLog) throw new TProtocolException("Required field 'currentConstructedCommitLog' was not found in serialized data for struct CommitLogInfo")
    new Immutable(
      currentProcessedCommitLog,
      currentConstructedCommitLog,
      if (_passthroughFields == null)
        NoPassthroughFields
      else
        _passthroughFields.result()
    )
  }

  def apply(
    currentProcessedCommitLog: Long,
    currentConstructedCommitLog: Long
  ): CommitLogInfo =
    new Immutable(
      currentProcessedCommitLog,
      currentConstructedCommitLog
    )

  def unapply(_item: CommitLogInfo): _root_.scala.Option[_root_.scala.Tuple2[Long, Long]] = _root_.scala.Some(_item.toTuple)


  @inline private def readCurrentProcessedCommitLogValue(_iprot: TProtocol): Long = {
    _iprot.readI64()
  }

  @inline private def writeCurrentProcessedCommitLogField(currentProcessedCommitLog_item: Long, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(CurrentProcessedCommitLogField)
    writeCurrentProcessedCommitLogValue(currentProcessedCommitLog_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeCurrentProcessedCommitLogValue(currentProcessedCommitLog_item: Long, _oprot: TProtocol): Unit = {
    _oprot.writeI64(currentProcessedCommitLog_item)
  }

  @inline private def readCurrentConstructedCommitLogValue(_iprot: TProtocol): Long = {
    _iprot.readI64()
  }

  @inline private def writeCurrentConstructedCommitLogField(currentConstructedCommitLog_item: Long, _oprot: TProtocol): Unit = {
    _oprot.writeFieldBegin(CurrentConstructedCommitLogField)
    writeCurrentConstructedCommitLogValue(currentConstructedCommitLog_item, _oprot)
    _oprot.writeFieldEnd()
  }

  @inline private def writeCurrentConstructedCommitLogValue(currentConstructedCommitLog_item: Long, _oprot: TProtocol): Unit = {
    _oprot.writeI64(currentConstructedCommitLog_item)
  }


  object Immutable extends ThriftStructCodec3[CommitLogInfo] {
    override def encode(_item: CommitLogInfo, _oproto: TProtocol): Unit = { _item.write(_oproto) }
    override def decode(_iprot: TProtocol): CommitLogInfo = CommitLogInfo.decode(_iprot)
    override lazy val metaData: ThriftStructMetaData[CommitLogInfo] = CommitLogInfo.metaData
  }

  /**
   * The default read-only implementation of CommitLogInfo.  You typically should not need to
   * directly reference this class; instead, use the CommitLogInfo.apply method to construct
   * new instances.
   */
  class Immutable(
      val currentProcessedCommitLog: Long,
      val currentConstructedCommitLog: Long,
      override val _passthroughFields: immutable$Map[Short, TFieldBlob])
    extends CommitLogInfo {
    def this(
      currentProcessedCommitLog: Long,
      currentConstructedCommitLog: Long
    ) = this(
      currentProcessedCommitLog,
      currentConstructedCommitLog,
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
      val currentProcessedCommitLog: Long,
      val currentConstructedCommitLog: Long,
      override val _passthroughFields: immutable$Map[Short, TFieldBlob])
    extends CommitLogInfo {

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
   * This Proxy trait allows you to extend the CommitLogInfo trait with additional state or
   * behavior and implement the read-only methods from CommitLogInfo using an underlying
   * instance.
   */
  trait Proxy extends CommitLogInfo {
    protected def _underlying_CommitLogInfo: CommitLogInfo
    override def currentProcessedCommitLog: Long = _underlying_CommitLogInfo.currentProcessedCommitLog
    override def currentConstructedCommitLog: Long = _underlying_CommitLogInfo.currentConstructedCommitLog
    override def _passthroughFields = _underlying_CommitLogInfo._passthroughFields
  }
}

trait CommitLogInfo
  extends ThriftStruct
  with _root_.scala.Product2[Long, Long]
  with HasThriftStructCodec3[CommitLogInfo]
  with java.io.Serializable
{
  import CommitLogInfo._

  def currentProcessedCommitLog: Long
  def currentConstructedCommitLog: Long

  def _passthroughFields: immutable$Map[Short, TFieldBlob] = immutable$Map.empty

  def _1 = currentProcessedCommitLog
  def _2 = currentConstructedCommitLog

  def toTuple: _root_.scala.Tuple2[Long, Long] = {
    (
      currentProcessedCommitLog,
      currentConstructedCommitLog
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
                writeCurrentProcessedCommitLogValue(currentProcessedCommitLog, _oprot)
                _root_.scala.Some(CommitLogInfo.CurrentProcessedCommitLogField)
              } else {
                _root_.scala.None
              }
            case 2 =>
              if (true) {
                writeCurrentConstructedCommitLogValue(currentConstructedCommitLog, _oprot)
                _root_.scala.Some(CommitLogInfo.CurrentConstructedCommitLogField)
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
  def setField(_blob: TFieldBlob): CommitLogInfo = {
    var currentProcessedCommitLog: Long = this.currentProcessedCommitLog
    var currentConstructedCommitLog: Long = this.currentConstructedCommitLog
    var _passthroughFields = this._passthroughFields
    _blob.id match {
      case 1 =>
        currentProcessedCommitLog = readCurrentProcessedCommitLogValue(_blob.read)
      case 2 =>
        currentConstructedCommitLog = readCurrentConstructedCommitLogValue(_blob.read)
      case _ => _passthroughFields += (_blob.id -> _blob)
    }
    new Immutable(
      currentProcessedCommitLog,
      currentConstructedCommitLog,
      _passthroughFields
    )
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is removed
   * from the passthroughFields map, if present.
   */
  def unsetField(_fieldId: Short): CommitLogInfo = {
    var currentProcessedCommitLog: Long = this.currentProcessedCommitLog
    var currentConstructedCommitLog: Long = this.currentConstructedCommitLog

    _fieldId match {
      case 1 =>
        currentProcessedCommitLog = 0L
      case 2 =>
        currentConstructedCommitLog = 0L
      case _ =>
    }
    new Immutable(
      currentProcessedCommitLog,
      currentConstructedCommitLog,
      _passthroughFields - _fieldId
    )
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is removed
   * from the passthroughFields map, if present.
   */
  def unsetCurrentProcessedCommitLog: CommitLogInfo = unsetField(1)

  def unsetCurrentConstructedCommitLog: CommitLogInfo = unsetField(2)


  override def write(_oprot: TProtocol): Unit = {
    CommitLogInfo.validate(this)
    _oprot.writeStructBegin(Struct)
    writeCurrentProcessedCommitLogField(currentProcessedCommitLog, _oprot)
    writeCurrentConstructedCommitLogField(currentConstructedCommitLog, _oprot)
    if (_passthroughFields.nonEmpty) {
      _passthroughFields.values.foreach { _.write(_oprot) }
    }
    _oprot.writeFieldStop()
    _oprot.writeStructEnd()
  }

  def copy(
    currentProcessedCommitLog: Long = this.currentProcessedCommitLog,
    currentConstructedCommitLog: Long = this.currentConstructedCommitLog,
    _passthroughFields: immutable$Map[Short, TFieldBlob] = this._passthroughFields
  ): CommitLogInfo =
    new Immutable(
      currentProcessedCommitLog,
      currentConstructedCommitLog,
      _passthroughFields
    )

  override def canEqual(other: Any): Boolean = other.isInstanceOf[CommitLogInfo]

  private def _equals(x: CommitLogInfo, y: CommitLogInfo): Boolean =
      x.productArity == y.productArity &&
      x.productIterator.sameElements(y.productIterator)

  override def equals(other: Any): Boolean =
    canEqual(other) &&
      _equals(this, other.asInstanceOf[CommitLogInfo]) &&
      _passthroughFields == other.asInstanceOf[CommitLogInfo]._passthroughFields

  override def hashCode: Int = _root_.scala.runtime.ScalaRunTime._hashCode(this)

  override def toString: String = _root_.scala.runtime.ScalaRunTime._toString(this)


  override def productArity: Int = 2

  override def productElement(n: Int): Any = n match {
    case 0 => this.currentProcessedCommitLog
    case 1 => this.currentConstructedCommitLog
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productPrefix: String = "CommitLogInfo"

  def _codec: ThriftStructCodec3[CommitLogInfo] = CommitLogInfo
}