package astraea.spark.avro

import java.nio.ByteBuffer

import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.annotation.tailrec
import scala.collection.JavaConverters._


/**
 * Base class for expressions converting to/from spark InternalRow and GenericRecord.
 * @author sfitch
 * @since 2/23/17
 */
abstract class SparkRecordCodingExpression[Input, Output]
  extends UnaryExpression with NonSQLExpression with CodegenFallback with Serializable {

  override protected final def nullSafeEval(input: Any): Output = {
    val row = input.asInstanceOf[Input]
    convertRecord(row, SchemaPair(sparkSchema, avroSchema))
  }

  @tailrec
  protected final def convertRecord(input: Input, schema: SchemaPair): Output = {
    if(schema.isUnion) {
      val (newInput, newSchema) = resolveUnion(input, schema)
      convertRecord(newInput, newSchema)
    }
    else {
      val fieldData = schema.fields
        .map { case (selector, fieldSchema) ⇒
          val fieldValue = extractValue(input, selector, fieldSchema)
          (selector, convertField(fieldValue, fieldSchema))
        }

      constructOutput(fieldData, schema)
    }
  }

  protected def sparkSchema: StructType

  protected def avroSchema: Schema

  protected def extractValue(input: Input, sel: FieldSelector, schema: SchemaPair): Any

  protected def convertField(data: Any, schema: SchemaPair): Any

  protected def resolveUnion(input: Input, schema: SchemaPair): (Input, SchemaPair)

  protected def constructOutput(fieldData: Seq[(FieldSelector, Any)], schema: SchemaPair): Output
}

/** Expression converting Avro record format into the internal Spark format. */
case class AvroToSpark[T: AvroRecordCodec](child: Expression, sparkSchema: StructType)
  extends SparkRecordCodingExpression[GenericRecord, InternalRow] {

  def dataType: DataType = sparkSchema

  protected def avroSchema: Schema = implicitly[AvroRecordCodec[T]].schema

  protected def extractValue(input: GenericRecord, sel: FieldSelector, schema: SchemaPair): Any =
    input.get(sel.name)

  protected def convertField(data: Any, schema: SchemaPair): Any = (data, schema.spark) match {
    case (null, _) ⇒ null
    case (av, _: NumericType) ⇒ av
    case (sv, _: StringType) ⇒ UTF8String.fromString(String.valueOf(sv))
    case (bv, _: BooleanType) ⇒ bv
    case (rv: GenericRecord, st: StructType) ⇒ convertRecord(rv, SchemaPair(st, schema.avro))
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
    case (v, t) ⇒
      throw new NotImplementedError(s"Mapping '${v}' to '${t}' needs to be implemented.")
  }

  protected def constructOutput(fieldData: Seq[(FieldSelector, Any)], schema: SchemaPair): InternalRow =
    new GenericInternalRow(fieldData.map(_._2).toArray)

  override protected def otherCopyArgs: Seq[AnyRef] = implicitly[AvroRecordCodec[T]] :: Nil

  protected def resolveUnion(input: GenericRecord, schema: SchemaPair): (GenericRecord, SchemaPair) = {

    require(schema.isUnion)
    val schemaIndex = schema.avro.getIndexNamed(input.getSchema.getFullName)
    require(schemaIndex != null,
      s"Couldn't find union type with name '${input.getSchema.getName}' in ${schema.avro}")

    val selectedAvro = schema.avro.getTypes.get(schemaIndex)
    val sparkField = schema.sparkStruct.fields(schemaIndex)

    val builder = SchemaBuilder
      .record(selectedAvro.getName + "ResolvedUnion").namespace(selectedAvro.getNamespace)
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

    rec.put(sparkField.name, input)

    (rec, schema.copy(avro = newSchema))
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
    case (sv, _: StringType) ⇒ sv.toString
    case (bv, _: BooleanType) ⇒ bv
    case (rv: InternalRow, st: StructType) ⇒ convertRecord(rv, SchemaPair(st, schema.avro))
    case (bv: Array[Byte], _: BinaryType) ⇒ ByteBuffer.wrap(bv)
    case (av: ArrayData, at: ArrayType) ⇒
      val elementType = at.elementType
      elementType match {
        case st: StructType ⇒
          val avroArray = new GenericData.Array[GenericRecord](av.numElements(), schema.avro)
          val avroElementType = schema.avro.getElementType
          av.foreach(elementType, {
            case (_, element: InternalRow) ⇒
              val convertedElement = convertRecord(element, SchemaPair(st, avroElementType))
              avroArray.add(convertedElement)
          })
          avroArray
        case _ ⇒
          throw new NotImplementedError(
            s"Mapping '${av}' with element '${elementType}' to '${at}' needs to be implemented.")
      }

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

  protected def resolveUnion(input: InternalRow, schema: SchemaPair) = {
    require(schema.isUnion)

    val Some((sparkField, schemaIndex)) = schema.sparkStruct.fields
      .zipWithIndex
      .find(p ⇒ !input.isNullAt(p._2))

    val sparkType = sparkField.dataType
    val avroType = schema.avro.getTypes.get(schemaIndex)

    (input, SchemaPair(sparkType, avroType))
  }
}
