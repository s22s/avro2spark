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
 * @author sfitch (@metasim)
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
