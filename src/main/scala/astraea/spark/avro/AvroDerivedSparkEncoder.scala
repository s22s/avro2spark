package astraea.spark.avro

import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.types._

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
  private[avro] def serializerFor[T: AvroRecordCodec: TypeTag](input: Expression, schema: StructType): Expression =
    AvroToSpark(EncodeToAvro(input), schema)

  /** Constructs the deserialization expression. */
  private[avro] def deserializerFor[T : AvroRecordCodec: TypeTag](input: Expression, schema: StructType): Expression =
    DecodeFromAvro(SparkToAvro(input, schema))
}
