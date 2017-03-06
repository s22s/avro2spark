package astraea.spark.avro

import java.nio.ByteBuffer
import java.util
import java.util.UUID

import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.Try


/**
 * Base class for expressions converting to/from spark InternalRow and GenericRecord.
 * @author sfitch (@metasim)
 * @since 2/23/17
 */
abstract class SparkRecordCodingExpression[Input >: Null, Output]
  extends UnaryExpression with NonSQLExpression with CodegenFallback with Serializable {

  override protected final def nullSafeEval(input: Any): Output = {
    val row = input.asInstanceOf[Input]
    convertRecord(row, SchemaPair(sparkSchema, avroSchema))
  }

  @tailrec
  protected final def convertRecord(input: Input, schema: SchemaPair): Output = {
    if(schema.isUnion) {
      // If MatchError, then error.
      val Some(idx) = unionIndex(input, schema)
      val (newInput, newSchema) = resolveUnion[Input](input, idx, schema)
      convertRecord(newInput, newSchema)
    }
    else {
      val fieldData = schema.fields
        .map {
          case (selector, fieldSchema) ⇒ if (fieldSchema.isUnion) {
            fieldSchema.spark match {
              case _: StructType ⇒
                val fieldValue = extractValue(input, selector, fieldSchema)
                val Some(idx) = unionIndex(fieldValue, fieldSchema)
                //val newSchema = fieldSchema.selectUnion(idx)
                val (newInput, newSchema) = resolveUnion[Any](fieldValue, idx, fieldSchema)
                (selector, convertField(newInput, newSchema))
              case _ ⇒
                // This is intended to handle the case where the union is over a type or the null schema
                val fieldValue = extractValue(input, selector, fieldSchema)
                val Some(idx) = unionIndex(fieldValue, fieldSchema)
                val newSchema = fieldSchema.copy(avro = fieldSchema.avro.getTypes.get(idx))
                (selector, convertField(fieldValue, newSchema))
            }
          }
          else {
            val fieldValue = extractValue(input, selector, fieldSchema)
            (selector, convertField(fieldValue, fieldSchema))
          }
        }

      constructOutput(fieldData, schema)
    }
  }

  protected def sparkSchema: StructType

  protected def avroSchema: Schema

  protected def extractValue(input: Input, sel: FieldSelector, schema: SchemaPair): Any

  protected def convertField(data: Any, schema: SchemaPair): Any

  protected def unionIndex(data: Any, schema: SchemaPair): Option[Int]

  protected def resolveUnion[R >: Null](data: Any, unionIndex: Int, schema: SchemaPair): (R, SchemaPair)

  protected def constructOutput(fieldData: Seq[(FieldSelector, Any)], schema: SchemaPair): Output
}

/** Expression converting Avro record format into the internal Spark format. */
case class AvroToSpark[T: AvroRecordCodec](child: Expression, sparkSchema: StructType)
  extends SparkRecordCodingExpression[GenericRecord, InternalRow] {

  private val tmpNamespace = UUID.randomUUID().toString

  def dataType: DataType = sparkSchema

  protected def avroSchema: Schema = implicitly[AvroRecordCodec[T]].schema

  protected def extractValue(input: GenericRecord, sel: FieldSelector, schema: SchemaPair): Any =
    input.get(sel.name)

  protected def convertField(data: Any, schema: SchemaPair): Any = (data, schema.spark) match {
    case (null, _) ⇒ null
    case (sv, _: StringType) ⇒ UTF8String.fromString(String.valueOf(sv))
    case (rv: GenericRecord, st: StructType) ⇒ convertRecord(rv, SchemaPair(st, schema.avro))
    case (nv: java.lang.Number, nt: NumericType) ⇒ nt match {
      case _: ShortType ⇒ nv.shortValue()
      case _: IntegerType ⇒ nv.intValue()
      case _ ⇒ nv
    }
    case (bv, _: BooleanType) ⇒ bv
    case (av: ByteBuffer, _: ArrayType) ⇒ av.array()
    case (av: java.lang.Iterable[_], at: ArrayType) ⇒
      val avroElementType = schema.avro.getElementType
      val convertedValues = av.asScala.map(e ⇒ convertField(e, SchemaPair(at.elementType, avroElementType)))
      new GenericArrayData(convertedValues)
    case (bv: Fixed, bt: BinaryType) ⇒
      // Notes in spark-avro indicate that the buffer behind Fixed
      // is reused and needs to be cloned.
      bv.bytes().clone()
    case (bv: ByteBuffer, _: BinaryType) ⇒ bv.array()
    case (mv: java.util.Map[String @unchecked, _], mt: MapType) ⇒
      val avroElementType = schema.avro.getValueType
      val (keys, values) = (for {
        (k, v) ← mv.toSeq
      } yield (UTF8String.fromString(k), convertField(v, SchemaPair(mt.valueType, avroElementType))))
        .unzip
      new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))
    case (v, t) ⇒
      throw new NotImplementedError(s"Mapping '${v}' to '${t}' needs to be implemented.")
  }

  protected def constructOutput(fieldData: Seq[(FieldSelector, Any)], schema: SchemaPair): InternalRow =
    new GenericInternalRow(fieldData.map(_._2).toArray)

  override protected def otherCopyArgs: Seq[AnyRef] = implicitly[AvroRecordCodec[T]] :: Nil

  protected def unionIndex(data: Any, schema: SchemaPair): Option[Int] =
    Try(GenericData.get().resolveUnion(schema.avro, data)).toOption

  protected def resolveUnion[R >: Null](data: Any, unionIndex: Int, schema: SchemaPair): (R, SchemaPair) = {
    require(schema.isUnion)

    val selectedAvro = schema.avro.getTypes.get(unionIndex)
    val sparkField = schema.sparkStruct.fields(unionIndex)

    val builder = SchemaBuilder
      .record(selectedAvro.getName + "ResolvedUnion").namespace(tmpNamespace)
      .fields()

    val newSchema = schema.sparkStruct.fields.foldRight(builder) {
      // For everything except the selected schema, the actual type doesn't really matter.
      case (field, builder) ⇒
        if (field == sparkField)
          builder.name(field.name).`type`(selectedAvro).noDefault()
        else
          builder.name(field.name).`type`(selectedAvro).withDefault(null)
    }.endRecord()

    val rec = new GenericData.Record(newSchema)

    rec.put(sparkField.name, data)

    (rec.asInstanceOf[R], schema.copy(avro = newSchema))
  }
}

/** Expression converting the Spark internal representation into Avro records. */
case class SparkToAvro[T: AvroRecordCodec](child: Expression, sparkSchema: StructType)
  extends SparkRecordCodingExpression[InternalRow, GenericRecord] {

  def dataType: DataType = ScalaReflection.dataTypeFor[GenericRecord]

  protected def avroSchema: Schema = implicitly[AvroRecordCodec[T]].schema

  protected def extractValue(input: InternalRow, sel: FieldSelector, schema: SchemaPair): Any =
    input.get(sel.ordinal, schema.spark)

  protected def convertField(data: Any, schema: SchemaPair): Any = (data, schema.spark) match {
    case (null, _) ⇒ null
    case (av, _: NumericType) ⇒ av
    case (sv: UTF8String, _: StringType) ⇒ sv.toString
    case (bv, _: BooleanType) ⇒
      bv
    case (rv: InternalRow, st: StructType) ⇒ convertRecord(rv, SchemaPair(st, schema.avro))
    case (bv: Array[Byte], _: BinaryType) ⇒ ByteBuffer.wrap(bv)
    case (av: ArrayData, at: ArrayType) ⇒
      val elementType = schema.elementType
      elementType.spark match {
        case st: StructType ⇒
          val avroArray = new GenericData.Array[GenericRecord](av.numElements(), schema.avro)
          av.foreach(elementType.spark, {
            case (_, element: InternalRow) ⇒ avroArray.add(convertRecord(element, elementType))
          })
          avroArray
        case _ ⇒
          val avroArray = new GenericData.Array[Any](av.numElements(), schema.avro)
          av.foreach(elementType.spark, {
            case (_, element) ⇒ avroArray.add(convertField(element, schema.elementType))
          })
          avroArray
      }

    case (mv: MapData, mt: MapType) ⇒
      val valueSchema = SchemaPair(mt.valueType, schema.avro.getValueType)
      val avroMap = new util.HashMap[String, Any]()
      mv.foreach(mt.keyType, mt.valueType, {
        case (key: UTF8String, value: InternalRow) ⇒
          val convertedElement = convertRecord(value, valueSchema)
          avroMap.put(key.toString, convertedElement)
        case (key: UTF8String, value: UTF8String) ⇒
          avroMap.put(key.toString, value.toString)
      })
      avroMap
    case (v, t) ⇒
      throw new NotImplementedError(s"Mapping '${v}' to '${t}' needs to be implemented.")
  }

  protected def constructOutput(fieldData: Seq[(FieldSelector, Any)], schema: SchemaPair): GenericRecord = {
    val rec = new GenericData.Record(schema.avro)
    fieldData.foreach { case (sel, value) ⇒
      rec.put(sel.name, value)
    }
    rec
  }

  override protected def otherCopyArgs: Seq[AnyRef] = implicitly[AvroRecordCodec[T]] :: Nil

  protected def unionIndex(data: Any, schema: SchemaPair): Option[Int] = data match {
    case input: InternalRow ⇒
      val numFields = schema.sparkStruct.fields.length
      (0 until numFields).find(!input.isNullAt(_))
    case _ ⇒
      Try(GenericData.get().resolveUnion(schema.avro, data)).toOption
  }

  protected def resolveUnion[R >: Null](data: Any, unionIndex: Int, schema: SchemaPair) = {
    require(schema.isUnion)

    val sparkField = schema.sparkStruct.fields(unionIndex)
    val sparkType = sparkField.dataType
    val avroType = schema.avro.getTypes.get(unionIndex)

    val resolvedInput = data match {
      case input: InternalRow ⇒ input.get(unionIndex, sparkType).asInstanceOf[R]
      case _ ⇒
        throw new IllegalArgumentException("Didn't expect to have to resolve union against " + data.getClass.getName)
    }

    (resolvedInput, SchemaPair(sparkType, avroType))
  }
}
