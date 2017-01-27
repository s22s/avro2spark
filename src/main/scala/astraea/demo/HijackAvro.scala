package astraea.demo

import java.time.ZonedDateTime

import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteConstantTile, MultibandTile}
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.vector.Extent
import org.apache.avro.Schema
import org.apache.avro.Schema.{Field, Type}
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

/**
 *
 * @author sfitch 
 * @since 1/26/17
 */
object HijackAvro extends TemporalProjectedExtentCodec {

  def codecOf[T: AvroRecordCodec](t: T) = implicitly[AvroRecordCodec[T]]

  type UnionResolver = Schema ⇒ Schema
  val pickFirstUnionResolver = (s: Schema) ⇒ s.getTypes.head

  def buildMetadata(field: Field): Metadata = {
    val md = new MetadataBuilder()
    Option(field.doc()).foreach(md.putString("doc", _))

    val k = "default"
    Option(field.defaultVal()).foreach {
      case s: String ⇒ md.putString(k, s)
      case i: java.lang.Integer ⇒ md.putLong(k, i.longValue())
      case l: java.lang.Long ⇒ md.putLong(k, l)
      case b: java.lang.Boolean ⇒ md.putBoolean(k, b)
      case d: java.lang.Double ⇒ md.putDouble(k, d)
      case f: java.lang.Float ⇒ md.putDouble(k, f.doubleValue())
    }

    Option(field.aliases())
      .filter(_.nonEmpty)
      .map(_.toSeq.toArray)
      .foreach(md.putStringArray("aliases", _))
    md.build()
  }

  def convert(field: Field, ur: UnionResolver): StructField = {
    // Judgement call: If there's a default value, implies nullability.
    val nullable = Option(field.defaultVal()).isDefined

    StructField(
      field.name(),
      convert(field.schema(), ur),
      nullable,
      buildMetadata(field)
    )
  }


  def convert(schema: Schema, ur: UnionResolver): DataType = {
    schema.getType match {
      case Type.RECORD ⇒ StructType(schema.getFields.sortBy(_.pos()).map(convert(_, ur)))
      case Type.ARRAY ⇒ ArrayType(convert(schema.getElementType, ur))
      case Type.UNION ⇒
        val selected = ur(schema)
        require(selected.getType != Type.UNION, "You were supposed to make union type go away...")
        convert(selected, ur)
      case Type.STRING ⇒ StringType
      case Type.DOUBLE ⇒ DoubleType
      case Type.INT ⇒ IntegerType
      case Type.BOOLEAN ⇒ BooleanType
      case Type.BYTES ⇒ ByteType
      case Type.LONG ⇒ LongType
      case Type.FLOAT ⇒ FloatType
      case t ⇒ throw new org.apache.avro.AvroTypeException(s"Unable to convert Avro type '$t' to Spark DataType")
    }
  }

  def main(args: Array[String]): Unit = {

    val tpe = TemporalProjectedExtent(Extent(0.0, 0.0, 8.0, 10.0), LatLng, ZonedDateTime.now())

    val tile = MultibandTile(
      ByteConstantTile(1, 10, 10),
      ByteConstantTile(2, 10, 10),
      ByteConstantTile(3, 10, 10)
    )

    val target = (tpe, tile)

    val codec = codecOf(target)

    println(codec.schema.toString(true))

    println("*" * 50)

    // TODO: Not sure how to work this out.
    val unionResolver: UnionResolver = (s: Schema) ⇒
      s.getTypes.find(_.getName.contains("Byte")).getOrElse(s.getTypes.head)

    val converted = convert(codec.schema, unionResolver)

    println(converted.prettyJson)
  }
}
