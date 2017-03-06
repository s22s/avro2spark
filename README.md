# avro2spark

 [ ![Download](https://api.bintray.com/packages/s22s/maven/avro2spark/images/download.svg) ](https://bintray.com/s22s/maven/avro2spark/_latestVersion)

Experimental facility for encoding GeoTrellis types into Spark Datasets/Dataframes. 

To use:

1. `import geotrellis.spark.io._`
2. Create an implicit encoder for your type which has a `AvroRecordCodec[T] defined for it. e.g.:

        implicit val tileEncoder = AvroDerivedSparkEncoder[Tile]


## Resources

* Paper: [Spark SQL: Relational Data Processing in Spark](http://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf)



