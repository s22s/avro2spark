package astraea.demo

import geotrellis.spark.io.avro.AvroRecordCodec
import org.scalatest.{FunSpec, Matchers}
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import scala.reflect.runtime.universe._

/**
 *
 * @author sfitch 
 * @since 2/13/17
 */
class AvroDerivedSparkEncoderSpec extends FunSpec with Matchers {

  def encoderOf[T: AvroRecordCodec: TypeTag] = AvroDerivedSparkEncoder[T].asInstanceOf[ExpressionEncoder[T]]

  describe("Avro-derived encoding") {
    it("should handle Extent") {
      val extent = Extent(1, 2, 3, 4)

      val enc = encoderOf[Extent]
      val row = enc.toRow(extent)
      println(row.getClass)
      println(row)
    }
  }
}
