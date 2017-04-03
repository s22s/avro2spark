/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright (c) 2017. Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
 * @author sfitch (@metasim)
 * @since 2/7/17
 */
object AvroDerivedSparkEncoder {

  /** Main entry point for creating a Spark `Encoder` from an implicitly available `AvroRecordCodec`. */
  def apply[T: AvroRecordCodec: TypeTag]: Encoder[T] = {

    val cls = classTagFor[T]

    // Get the `spark-avro` generated schema
    val schema = SchemaConverter.schemaFor[T]

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
    // We always write to a single column.
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
