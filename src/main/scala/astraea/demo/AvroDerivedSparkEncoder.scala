package astraea.demo

import java.nio.ByteBuffer

import com.databricks.spark.avro.SchemaConverters
import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericInternalRow, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Spark SQL [[Encoder]] derived from an existing GeoTrellis [[AvroRecordCodec]].
 *
 * @author sfitch 
 * @since 2/7/17
 */
object AvroDerivedSparkEncoder {

  /** Main entry point for creating a Spark `Encoder` from an implicitly available `AvroRecordCodec`. */
  def apply[T: AvroRecordCodec: TypeTag]: Encoder[T] = {

    val cls = classTagFor[T]

    // Get the `spark-avro` generated schema
    val schema = schemaFor[T]

    // The results from serialization will be all in one column. This introduces a parent schema
    // to communicate this grouping mechanism to Catalyst.
    val wrappedSchema = StructType(Seq(StructField(fieldNameFor[T], schema)))

    // The Catalyst mechanism for creating a to-be-resolved column dependency. This is the input for
    // the rest of the expression pipeline.
    val inputObject = BoundReference(0, dataTypeFor[T], nullable = false)

    val serializer = serializerFor[T](inputObject, schema)

    val deserializer = deserializerFor[T](schema)

    // Not sure what this is.... assuming it means that serialization results are spread out over multiple columns.
    val flat = false

    // As of Spark 2.1.0, the `ExpressionEncoder` API is the only publicly accessible means of creating
    // RDD => Dataframe converters. This means using the Catalyst `Expression` API, where an execution
    // plan is defined in AST-like form.
    ExpressionEncoder[T](
      wrappedSchema,
      flat,
      Seq(serializer),
      deserializer,
      cls
    )
  }

  /** Constructs the serialization expression. */
  def serializerFor[T: AvroRecordCodec: TypeTag](inputObject: Expression, schema: StructType): Expression =
    AvroToSpark(EncodeToAvro(inputObject), schema)

  /** Constructs the deserialization expression. */
  def deserializerFor[T : AvroRecordCodec: TypeTag](schema: StructType): Expression =
    DecodeFromAvro(SparkToAvro(GetColumnByOrdinal(0, schema), schema))

  /** Expression to convert an incoming expression of type `T` to an Avro [[GenericRecord]]. */
  case class EncodeToAvro[T: AvroRecordCodec](child: Expression)
    extends UnaryExpression with NonSQLExpression with CodegenFallback {

    val codec = implicitly[AvroRecordCodec[T]]

    override def dataType: DataType = dataTypeFor[GenericRecord]

    override def nullable: Boolean = false

    override protected def nullSafeEval(input: Any): GenericRecord = {
      val obj = input.asInstanceOf[T]
      val result = codec.encode(obj)
      // XXX: The problem here is that if a union type is involved, the intermediate
      // array representation is removed in the value but not in the Spark schema.
      // See https://github.com/databricks/spark-avro#supported-types-for-avro---spark-sql-conversion
      val avroSchema = codec.schema
      if(avroSchema.getType == Type.UNION) {
        println(s">> union misalignment: \n\t$result\n>> should conform to\n\t$avroSchema")
      }
      result
    }

    override protected def otherCopyArgs: Seq[AnyRef] = codec :: Nil
  }

  /** Expression taking input in Avro format and converting into native type `T`. */
  case class DecodeFromAvro[T: AvroRecordCodec: TypeTag](child: Expression)
    extends UnaryExpression with NonSQLExpression with CodegenFallback {

    val codec = implicitly[AvroRecordCodec[T]]

    override def dataType: DataType = dataTypeFor[T]

    override protected def nullSafeEval(input: Any): T = {
      val record = input.asInstanceOf[GenericRecord]
      codec.decode(record)
    }

    override protected def otherCopyArgs: Seq[AnyRef] =
      codec :: implicitly[TypeTag[T]] :: Nil
  }

  /** Expression converting Avro record format into the internal Spark format. */
  case class AvroToSpark(child: Expression, sparkSchema: StructType)
    extends UnaryExpression with NonSQLExpression with CodegenFallback {

    override def dataType: DataType = sparkSchema

    private def convertField(data: Any, fieldType: DataType): Any = (data, fieldType) match {
      case (null, _) ⇒ null
      case (av, _: NumericType) ⇒ av
      case (sv, _: StringType) ⇒ UTF8String.fromString(String.valueOf(sv))
      case (bv, _: BooleanType) ⇒ bv
      case (rv: GenericRecord, st: StructType) ⇒ convertRecord(rv, st)
      case (av: ByteBuffer, _: ArrayType) ⇒ av.array()
      case (av: java.lang.Iterable[_], at: ArrayType) ⇒
        new GenericArrayData(av.asScala.map(e ⇒ convertField(e, at.elementType)))
      case (bv: Fixed, bt: BinaryType) ⇒
        // Notes in spark-avro indicate that the buffer behind Fixed
        // is shared and needs to be cloned.
        bv.bytes().clone()
      case (bv: ByteBuffer, _: BinaryType) ⇒ bv.array()
      case (v, t) ⇒
        throw new NotImplementedError(s"Mapping '${v}' to '${t}' needs to be implemented.")
    }

    private val fieldConverter = (convertField _).tupled

    private def convertRecord(gr: GenericRecord, rowSchema: StructType): InternalRow = {
      val fieldData = gr.getSchema.getFields
        .map { field ⇒
          val avroValue = gr.get(field.name())
          val sparkType = rowSchema(field.name()).dataType
          (avroValue, sparkType)
        }
        .map(fieldConverter)
      new GenericInternalRow(fieldData.toArray)
    }

    override protected def nullSafeEval(input: Any): InternalRow = {
      val avroRecord = input.asInstanceOf[GenericRecord]
      convertRecord(avroRecord, sparkSchema)
    }
  }

  /** Expression converting the Spark internal representation into Avro records. */
  case class SparkToAvro[T: AvroRecordCodec](child: Expression, sparkSchema: StructType)
    extends UnaryExpression with NonSQLExpression with CodegenFallback {
    val codec = implicitly[AvroRecordCodec[T]]

    override def dataType: DataType = dataTypeFor[GenericRecord]

    private def convertField(data: Any, fieldType: DataType, avroSchema: Schema): Any = (data, fieldType) match {
      case (null, _) ⇒ null
      case (av, _: NumericType) ⇒ av
      case (sv, _: StringType) ⇒ sv.toString
      case (bv, _: BooleanType) ⇒ bv
      case (rv: InternalRow, st: StructType) ⇒ convertRow(rv, st, avroSchema)
      case (bv: Array[Byte], bt: BinaryType) ⇒ ByteBuffer.wrap(bv)
      case (av: ArrayData, at: ArrayType) ⇒
        val elementType = at.elementType
        elementType match {
          case st: StructType ⇒
            val avroArray = new GenericData.Array[GenericRecord](av.numElements(), avroSchema)
            av.foreach(elementType, {
              case (_, element: InternalRow) ⇒
                val convertedElement = convertRow(element, st, avroSchema.getElementType)
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

    private val fieldConverter = (convertField _).tupled

    private def convertRow(row: InternalRow, rowSchema: StructType, recordSchema: Schema): GenericRecord = {
      val fieldData = rowSchema.fields.zipWithIndex
        .map { case (field, i) ⇒
          val name = field.name
          val avroField = recordSchema.getField(name)
          val sparkValue = row.get(i, field.dataType)
          val sparkType = rowSchema(field.name).dataType
          (name, (sparkValue, sparkType, avroField.schema()))
        }
        .map(f ⇒ (f._1, fieldConverter(f._2)))

      val rec = new GenericData.Record(recordSchema)
      fieldData.foreach { case (name, value) ⇒
        rec.put(name, value)
      }
      rec
    }
    override protected def nullSafeEval(input: Any): GenericRecord = {
      val row = input.asInstanceOf[InternalRow]
      convertRow(row, sparkSchema, codec.schema)
    }

    override protected def otherCopyArgs: Seq[AnyRef] = codec :: Nil
  }

  /** Utility to build a ClassTag from a TypeTag. */
  private def classTagFor[T: TypeTag] = {
    val mirror = typeTag[T].mirror
    val tpe = typeTag[T].tpe
    val cls = mirror.runtimeClass(tpe)
    ClassTag[T](cls)
  }

  /**
   * Returns the Spark SQL DataType for a given scala type.  Where this is not an exact mapping
   * to a native type, an ObjectType is returned. Unlike `schemaFor`, this function doesn't do
   * any massaging of types into the Spark SQL type system.
   * As a result, ObjectType will be returned for things like boxed Integers
   * @see [[ScalaReflection.dataTypeFor]]
   */
  def dataTypeFor[T: TypeTag]: DataType = ScalaReflection.dataTypeFor[T]

  /** Via Databricks' `spark-avro`, convert the Avro schema into a Spark/Catalyst schema. */
  def schemaFor[T: AvroRecordCodec]: StructType = {
    val codec = implicitly[AvroRecordCodec[T]]

    // Convert Avro schema to Spark schema via Databricks library
    val sqlType = SchemaConverters.toSqlType(codec.schema)

    sqlType.dataType match {
      case st: StructType ⇒ st
      case dt ⇒ throw new IllegalArgumentException(s"$dt not a struct. Use built-in Spark `Encoder`s instead.")
    }
  }

  /** Generate a field name for the serialized object.  */
  def fieldNameFor[T: TypeTag]: String = classTagFor[T].runtimeClass.getSimpleName

}
