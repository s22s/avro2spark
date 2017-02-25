package astraea.spark

import com.databricks.spark.avro.SchemaConverters
import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.spark.sql.types.{DataType, StructType}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.collection.JavaConversions._

/**
 * Supporting cast of characters in Avro <-> Catalyst conversion.
 *
 * @author sfitch (@metasim)
 * @since 2/23/17
 */
package object avro {
  /** Pairing of field name with it's order in the record. */
  private[avro] case class FieldSelector(ordinal: Int, name: String)

  /** Synchronized pairing of Spark and Avro schemas. */
  private[avro] case class SchemaPair(spark: DataType, avro: Schema) {
    val isUnion = avro.getType == Type.UNION

    def sparkStruct = {
      require(spark.isInstanceOf[StructType], "Expected StructType, not " + spark)
      spark.asInstanceOf[StructType]
    }

    def fields: Seq[(FieldSelector, SchemaPair)] = spark match {
      case st: StructType ⇒
        st.fields.zip(avro.getFields).zipWithIndex.map {
          case ((sparkField, avroField), index) ⇒
            (FieldSelector(index, sparkField.name), SchemaPair(sparkField.dataType, avroField.schema()))
        }
      case _ ⇒ Seq.empty
    }
  }

  /** Utility to build a ClassTag from a TypeTag. */
  private[avro] def classTagFor[T: TypeTag] = {
    val mirror = typeTag[T].mirror
    val tpe = typeTag[T].tpe
    val cls = mirror.runtimeClass(tpe)
    ClassTag[T](cls)
  }

  /** Via Databricks' `spark-avro`, convert the Avro schema into a Spark/Catalyst schema. */
  private[avro] def schemaFor[T: AvroRecordCodec]: StructType = {
    val codec = implicitly[AvroRecordCodec[T]]

    // Convert Avro schema to Spark schema via Databricks library
    val sqlType = SchemaConverters.toSqlType(codec.schema)

    sqlType.dataType match {
      case st: StructType ⇒ st
      case dt ⇒ throw new IllegalArgumentException(s"$dt not a struct. Use built-in Spark `Encoder`s instead.")
    }
  }

  /** Generate a field name for the serialized object. Shows up as the default column name. */
  private[avro] def fieldNameFor[T: TypeTag]: String = classTagFor[T].runtimeClass.getSimpleName

}
