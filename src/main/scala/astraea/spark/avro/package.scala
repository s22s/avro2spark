package astraea.spark

import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.spark.sql.types.{StructType, _}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.collection.JavaConverters._

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

    /** Is an Avro union represented in this Schema. */
    val isUnion = avro.getType == Type.UNION

    def sparkStruct = spark match {
      case st: StructType ⇒ st
      case ot ⇒ throw new IllegalArgumentException("Expected StructType, not " + ot)
    }

    /** When this is a union, select the sub-schema at the given index in the union ordering. */
    def selectUnion(idx: Int): SchemaPair = {
      require(isUnion)
      val s = sparkStruct.fields(idx)
      val a = avro.getTypes.get(idx)
      SchemaPair(s.dataType, a)
    }

    def elementType: SchemaPair = spark match {
      case at: ArrayType ⇒ SchemaPair(at.elementType, avro.getElementType)
      case ot ⇒ throw new IllegalArgumentException("Expected ArrayType, not " + ot)
    }

    def fields: Seq[(FieldSelector, SchemaPair)] = spark match {
      case st: StructType ⇒
        st.fields.zip(avro.getFields.asScala).zipWithIndex.map {
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


  /** Generate a field name for the serialized object. Shows up as the default column name. */
  private[avro] def fieldNameFor[T: TypeTag]: String = classTagFor[T].runtimeClass.getSimpleName

}
