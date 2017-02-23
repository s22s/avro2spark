package astraea.demo

import java.nio.ByteBuffer
import javax.validation.UnexpectedTypeException

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

import scala.annotation.tailrec
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
    val inputObject = BoundReference(0, ScalaReflection.dataTypeFor[T], nullable = false)
    val serializer = serializerFor[T](inputObject, schema)

    val inputRow = GetColumnByOrdinal(0, schema)
    val deserializer = deserializerFor[T](inputRow, schema)

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
  def serializerFor[T: AvroRecordCodec: TypeTag](input: Expression, schema: StructType): Expression =
    AvroToSpark(EncodeToAvro(input), schema)

  /** Constructs the deserialization expression. */
  def deserializerFor[T : AvroRecordCodec: TypeTag](input: Expression, schema: StructType): Expression =
    DecodeFromAvro(SparkToAvro(input, schema))

  /** Pairing of field name with it's order in the record. */
  private[demo] case class FieldSelector(ordinal: Int, name: String)

  /** Synchronized pairing of Spark and Avro schemas. */
  private[demo] case class SchemaPair[+T <: DataType](spark: T, avro: Schema) {
    val isUnion = avro.getType == Type.UNION

    def fields: Seq[(FieldSelector, SchemaPair[DataType])] = spark match {
      case st: StructType ⇒
        st.fields.zip(avro.getFields).zipWithIndex.map {
          case ((sparkField, avroField), index) ⇒
            (FieldSelector(index, sparkField.name), SchemaPair(sparkField.dataType, avroField.schema()))
        }
      case _ ⇒ Seq.empty
    }
  }

  /** Base class for expressions converting to/from spark InternalRow and GenericRecord. */
  abstract class SparkRecordCodingExpression[Input, Output]
    extends UnaryExpression with NonSQLExpression with CodegenFallback with Serializable {

    override protected final def nullSafeEval(input: Any): Output = {
      val row = input.asInstanceOf[Input]
      convertRecord(row, SchemaPair(sparkSchema, avroSchema))
    }

    @tailrec
    protected final def convertRecord(input: Input, schema: SchemaPair[StructType]): Output = {
      if(schema.isUnion) {
        convertRecord(input, resolveUnion(input, schema))
      }
      else {
        val fieldData = schema.fields
          .map { case (selector, schema) ⇒
            val fieldValue = extractValue(input, selector, schema)
            (selector, convertField(fieldValue, schema))
          }

        constructOutput(fieldData, schema)
      }
    }

    protected def sparkSchema: StructType

    protected def avroSchema: Schema

    protected def extractValue(input: Input, sel: FieldSelector, schema: SchemaPair[DataType]): Any

    protected def convertField(data: Any, schema: SchemaPair[_]): Any

    protected def resolveUnion(input: Input, schema: SchemaPair[StructType]): SchemaPair[StructType]

    protected def constructOutput(fieldData: Seq[(FieldSelector, Any)], schema: SchemaPair[_]): Output
  }

  /** Expression converting Avro record format into the internal Spark format. */
  case class AvroToSpark[T: AvroRecordCodec](child: Expression, sparkSchema: StructType)
    extends SparkRecordCodingExpression[GenericRecord, InternalRow] {

    def dataType: DataType = sparkSchema

    protected def avroSchema: Schema = implicitly[AvroRecordCodec[T]].schema

    protected def extractValue(input: GenericRecord, sel: FieldSelector, schema: SchemaPair[DataType]): Any =
      input.get(sel.name)

    protected def convertField(data: Any, schema: SchemaPair[_]): Any = (data, schema.spark) match {
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
        // is shared and needs to be cloned.
        bv.bytes().clone()
      case (bv: ByteBuffer, _: BinaryType) ⇒ bv.array()
      case (v, t) ⇒
        throw new NotImplementedError(s"Mapping '${v}' to '${t}' needs to be implemented.")
    }

    protected def constructOutput(fieldData: Seq[(FieldSelector, Any)], schema: SchemaPair[_]): InternalRow =
      new GenericInternalRow(fieldData.map(_._2).toArray)

    override protected def otherCopyArgs: Seq[AnyRef] = implicitly[AvroRecordCodec[T]] :: Nil

    protected def resolveUnion(input: GenericRecord, schema: SchemaPair[StructType]): SchemaPair[StructType] = {
      require(schema.isUnion)
      val schemaIndex = schema.avro.getIndexNamed(input.getSchema.getFullName)
      require(schemaIndex != null,
        s"Couldn't find union type with name '${input.getSchema.getName}' in ${schema.avro}")

      val selectedAvro = schema.avro.getTypes.get(schemaIndex)
      val sparkField = schema.spark.fields(schemaIndex)
      // println(s"selected '${sparkField}' from '${schema.spark}'")

      sparkField.dataType match {
        case st: StructType ⇒ SchemaPair(st, selectedAvro)
        case _ ⇒ throw new UnexpectedTypeException(s"Avro union resolution failed for ${schema}:  ${input}")
      }
    }
  }

  /** Expression converting the Spark internal representation into Avro records. */
  case class SparkToAvro[T: AvroRecordCodec](child: Expression, sparkSchema: StructType)
    extends SparkRecordCodingExpression[InternalRow, GenericRecord] {

    def dataType: DataType = ScalaReflection.dataTypeFor[GenericRecord]

    protected def avroSchema: Schema = implicitly[AvroRecordCodec[T]].schema

    protected def extractValue(input: InternalRow, sel: FieldSelector, schema: SchemaPair[DataType]): Any =
      input.get(sel.ordinal, schema.spark)

    protected def convertField(data: Any, schema: SchemaPair[_]): Any = (data, schema.spark) match {
      case (null, _) ⇒ null
      case (av, _: NumericType) ⇒ av
      case (sv, _: StringType) ⇒ sv.toString
      case (bv, _: BooleanType) ⇒ bv
      case (rv: InternalRow, st: StructType) ⇒ convertRecord(rv, SchemaPair(st, schema.avro))
      case (bv: Array[Byte], bt: BinaryType) ⇒ ByteBuffer.wrap(bv)
      case (av: ArrayData, at: ArrayType) ⇒
        val elementType = at.elementType
        elementType match {
          case st: StructType ⇒
            val avroArray = new GenericData.Array[GenericRecord](av.numElements(), schema.avro)
            av.foreach(elementType, {
              case (_, element: InternalRow) ⇒
                val convertedElement = convertRecord(element, SchemaPair(st, schema.avro.getElementType))
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

    protected def constructOutput(fieldData: Seq[(FieldSelector, Any)], schema: SchemaPair[_]): GenericRecord = {
      val rec = new GenericData.Record(schema.avro)
      fieldData.foreach { case (sel, value) ⇒
        rec.put(sel.name, value)
      }
      rec
    }

    override protected def otherCopyArgs: Seq[AnyRef] = implicitly[AvroRecordCodec[T]] :: Nil

    protected def resolveUnion(input: InternalRow, schema: SchemaPair[StructType]): SchemaPair[StructType] = ???
  }

  /** Utility to build a ClassTag from a TypeTag. */
  private def classTagFor[T: TypeTag] = {
    val mirror = typeTag[T].mirror
    val tpe = typeTag[T].tpe
    val cls = mirror.runtimeClass(tpe)
    ClassTag[T](cls)
  }


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
