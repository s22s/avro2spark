import geotrellis.raster.{ByteConstantTile, MultibandTile}
import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.io.avro.codecs.Implicits._

def codecOf[T: AvroRecordCodec](t: T) = implicitly[AvroRecordCodec[T]]


val tile = MultibandTile(
  ByteConstantTile(1, 10, 10),
  ByteConstantTile(2, 10, 10),
  ByteConstantTile(3, 10, 10)
)

val codec = codecOf(tile)

val result = codec.encode(tile)


