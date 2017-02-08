import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.vector.Extent
import org.apache.avro.specific.SpecificDatumWriter

import scala.collection.JavaConversions._

def codecOf[T: AvroRecordCodec](t: T) = implicitly[AvroRecordCodec[T]]


//val codec = codecOf[Tile](null)
// codec.schema.getTypes
val ext = Extent(1, 2, 3, 4)
val codec = codecOf(ext)


codec.schema.getFields
  .sortBy(_.order())
  .map(f ⇒ f.name() + ": " + f.schema().getType)


val encoded = codec.encode(ext)

for {
  f ← codec.schema.getFields
} yield {
  encoded.get(f.name())
}
