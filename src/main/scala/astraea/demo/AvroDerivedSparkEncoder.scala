package astraea.demo

import java.nio.ByteBuffer

import com.databricks.spark.avro.SchemaConverters
import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericInternalRow, Literal, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
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
    val schema = schemaFor[T]

    // Now wondering if `flat` might mean that a single column is expanded into multiple ones?
    // ~~Assuming "flat" means all columns are primitive types. Not really sure.~~
    val flat = schema.fields.length == 1 && !schema.fields.head.dataType.isInstanceOf[StructType]

    val inputObject = BoundReference(0, dataTypeFor[T], nullable = false)

    val serializer = serializerFor[T](inputObject, schema)

    val deserializer = deserializerFor[T]

    val wrappedSchema = StructType(Seq(StructField(fieldNameFor[T], schema)))

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

  /** Intermediate expression to convert an incoming expression of type `T` to an Avro [[GenericRecord]]. */
  case class EncodeToAvro[T: AvroRecordCodec](child: Expression)
    extends UnaryExpression with NonSQLExpression with CodegenFallback {

    override def dataType: DataType = dataTypeFor[GenericRecord]

    override def nullable: Boolean = false

    override protected def nullSafeEval(input: Any): GenericRecord = {
      val obj = input.asInstanceOf[T]
      val codec = implicitly[AvroRecordCodec[T]]
      //println(">>>>>>> Encoding: " + obj)
      val result = codec.encode(obj)
      // XXX: The problem here is that if a union type is involved, the intermediate
      // array representation is removed in the value but not in the spark schema.
      result
    }

    override protected def otherCopyArgs: Seq[AnyRef] = implicitly[AvroRecordCodec[T]] :: Nil
  }

  case class AvroToSpark(child: Expression, sparkSchema: StructType)
    extends UnaryExpression with NonSQLExpression with CodegenFallback {

    override def dataType: DataType = sparkSchema

    private def convertAny(data: Any, fieldType: DataType): Any = (data, fieldType) match {
      case (av, _: NumericType) ⇒ av
      case (sv, _: StringType) ⇒ sv
      case (bv, _: BooleanType) ⇒ bv
      case (rv: GenericRecord, st: StructType) ⇒ convertRecord(rv, st)
      case (av: ByteBuffer, at: ArrayType) ⇒ av.array()
      case (av: java.lang.Iterable[_], at: ArrayType) ⇒
        av.asScala.map(e ⇒ convertAny(e, at.elementType))
      case (bv: Fixed, bt: BinaryType) ⇒
        // Notes in spark-avro indicate that the buffer behind Fixed is shared and needs to be cloned.
        bv.bytes().clone()
      case (bv: ByteBuffer, bt: BinaryType) ⇒ bv.array()
      case (v, t) ⇒
        throw new NotImplementedError(s"Mapping '${v}' to '${t}' needs to be implemented.")
    }

    private val fieldConverter = (convertAny _).tupled

    private def convertRecord(gr: GenericRecord, sparkSchema: StructType): InternalRow = {
      val fieldDataWithType = gr.getSchema.getFields
        .map { field ⇒

          println(field.name + " is " + field.schema())

          val spark = sparkSchema(field.name())
          println("nullable: " + spark.nullable)
          val fieldValue = gr.get(field.name())
          (fieldValue, spark.dataType)
        }
      new GenericInternalRow(fieldDataWithType.map(fieldConverter).toArray)
    }

    override protected def nullSafeEval(input: Any): InternalRow = {
      val avroRecord = input.asInstanceOf[GenericRecord]
      // We can't rely on the already computed Spark schema (as declared by DataType)
      // because UNION types result in a change in structure
      val sqlType = SchemaConverters.toSqlType(avroRecord.getSchema)


      convertRecord(avroRecord, sqlType.dataType.asInstanceOf[StructType])
    }
  }

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

  def schemaFor[T: AvroRecordCodec]: StructType = {
    val codec = implicitly[AvroRecordCodec[T]]

    // Convert Avro schema to Spark schema via Databricks library
    val sqlType = SchemaConverters.toSqlType(codec.schema)

    sqlType.dataType match {
      case st: StructType ⇒ st
      case dt ⇒ throw new IllegalArgumentException(s"$dt not a struct. Use built-in Spark `Encoder`s instead.")
    }
  }

  def fieldNameFor[T: TypeTag]: String = classTagFor[T].runtimeClass.getSimpleName

  def serializerFor[T: AvroRecordCodec: TypeTag](
    inputObject: Expression, schema: StructType): Expression = {
    val asAvro = EncodeToAvro(inputObject)
    val asSpark = AvroToSpark(asAvro, schema)
    asSpark
  }

  // TODO: This definitely needs to be defined properly for encoder chaining to work (e.g. tuples or products)
  def deserializerFor[T : AvroRecordCodec]: Expression = Literal("TODO")

  // See:
  /*
  org.apache.spark.sql.catalyst.ScalaReflection#serializerFor
   */

}
