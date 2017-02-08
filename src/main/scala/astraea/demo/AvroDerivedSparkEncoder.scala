package astraea.demo

import com.databricks.spark.avro.SchemaConverters
import com.databricks.spark.avro.hack.SchemaConvertersBackdoor
import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericRow, Literal, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

/**
 * Spark SQL [[Encoder]] derived from an existing GeoTrellis [[AvroRecordCodec]].
 *
 * @author sfitch 
 * @since 2/7/17
 */
object AvroDerivedSparkEncoder {

  /** Main entry point for creating a Spark `Encoder` from an implicitly available `AvroRecordCodec`. */
  def apply[T: AvroRecordCodec: ClassTag]: Encoder[T] = {

    val codec = implicitly[AvroRecordCodec[T]]

    // Convert Avro schema to Spark schema via Databricks library
    val sparkSchema = SchemaConverters.toSqlType(codec.schema).dataType match {
        case st: StructType ⇒ st
        case dt ⇒ throw new IllegalArgumentException(s"$dt not a struct. Use built-in Spark `Encoder`s instead.")
    }

    // Now wondering if `flat` might mean that a single column is expanded into multiple ones?
    // ~~Assuming "flat" means all columns are primitive types. Not really sure.~~
    val isFlat = sparkSchema.fields.length == 1 && !sparkSchema.fields.head.dataType.isInstanceOf[StructType]

    // As of Spark 2.1.0, the `ExpressionEncoder` API is the only publicly accessible means of creating
    // RDD => Dataframe converters. This means using the Catalyst `Expression` API, where an execution plan is
    // defined in AST-like form.
    ExpressionEncoder[T](
      schema = sparkSchema,
      flat = isFlat,
      serializer = {
        val br = BoundReference(0, sparkDataType[T], false)
        val asAvro = EncodeToAvro(br)
        sparkSchema.fields.map(f ⇒ ExtractFromAvro(asAvro, f))
      },
      deserializer = Literal(null), // TODO? or not necessary?
      clsTag = implicitly[ClassTag[T]])
  }

  /** Intermediate expression to convert an incoming expression of type `T` to an Avro [[GenericRecord]]. */
  case class EncodeToAvro[T: AvroRecordCodec: ClassTag](child: Expression)
    extends UnaryExpression with NonSQLExpression with CodegenFallback {

    override def dataType: DataType = sparkDataType[GenericRecord] // BinaryType

    override def nullable: Boolean = false

    override protected def nullSafeEval(input: Any): GenericRecord = {
      val codec = implicitly[AvroRecordCodec[T]]
      val obj = input.asInstanceOf[T]
      codec.encode(obj)
    }
  }

  /** Expression for pulling a specific field out of */
  case class ExtractFromAvro(child: Expression, sparkSchema: StructField)
    extends UnaryExpression with NonSQLExpression with CodegenFallback {

    override protected def nullSafeEval(input: Any): Any = {
      val avroRecord = input.asInstanceOf[GenericRecord]

      val fieldValue = avroRecord.get(sparkSchema.name)

      dataType match {
        case nestedSchema: StructType ⇒
          val nestedRecord = fieldValue.asInstanceOf[GenericRecord]
          val converter = SchemaConvertersBackdoor.createConverterToSQL(nestedRecord.getSchema, nestedSchema)
          val genericRow = converter(nestedRecord).asInstanceOf[GenericRow]
          val sqlRowlEncoder = RowEncoder(nestedSchema)
          sqlRowlEncoder.toRow(genericRow)

        case _ ⇒ fieldValue
      }
    }

    override def dataType: DataType = sparkSchema.dataType

    override def nullable: Boolean = false

  }

  /** Hack to get spark to create an `ObjectType` for us. */
  private def sparkDataType[T: ClassTag]: DataType = {
    import org.apache.spark.sql.catalyst.dsl.expressions._
    val clsTag = implicitly[ClassTag[T]]
    DslSymbol(Symbol(clsTag.toString())).obj(clsTag.runtimeClass).dataType
  }
}
