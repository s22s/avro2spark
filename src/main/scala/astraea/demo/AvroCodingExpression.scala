package astraea.demo

import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

import scala.reflect.runtime.universe._

/**
 * Spark/Catalyst expressions associated with converting to/from native types
 * from/to Avro records.
 *
 * @author sfitch 
 * @since 2/22/17
 */
abstract class AvroCodingExpression[T: AvroRecordCodec: TypeTag]
  extends UnaryExpression with NonSQLExpression with CodegenFallback with Serializable {

  val codec = implicitly[AvroRecordCodec[T]]
  val typeTag = implicitly[TypeTag[T]]

  override def nullable: Boolean = false

  override protected def otherCopyArgs: Seq[AnyRef] = codec :: typeTag :: Nil
}

/** Expression to convert an incoming expression of type `T` to an Avro [[GenericRecord]]. */
case class EncodeToAvro[T: AvroRecordCodec: TypeTag](child: Expression)
  extends AvroCodingExpression[T] {

  override def dataType: DataType = ScalaReflection.dataTypeFor[GenericRecord]

  // XXX: The problem here is that if a union type is involved, the intermediate
  // array representation is removed in the value but not in the Spark schema.
  // See https://github.com/databricks/spark-avro#supported-types-for-avro---spark-sql-conversion
  override protected def nullSafeEval(input: Any): GenericRecord =
  codec.encode(input.asInstanceOf[T])
}

/** Expression taking input in Avro format and converting into native type `T`. */
case class DecodeFromAvro[T: AvroRecordCodec: TypeTag](child: Expression)
  extends AvroCodingExpression[T] {

  override def dataType: DataType = ScalaReflection.dataTypeFor[T]

  override protected def nullSafeEval(input: Any): T =
    codec.decode(input.asInstanceOf[GenericRecord])
}
