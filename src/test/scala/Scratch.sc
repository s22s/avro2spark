import geotrellis.raster.{ArrayMultibandTile, ByteConstantTile, MultibandTile, Tile}
import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.vector.Extent
import org.apache.avro.{Schema, SchemaBuilder}

import scala.collection.JavaConversions._

def codecOf[T: AvroRecordCodec](t: T) = implicitly[AvroRecordCodec[T]]


//val codec = codecOf[Tile](null)
// codec.schema.getTypes

val codec = codecOf(Extent(1, 2, 3, 4))


codec.schema.getFields
  .sortBy(_.order())
  .map(f ⇒ f.name() + ": " + f.schema().getType)

