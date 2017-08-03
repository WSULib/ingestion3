package dpla.ingestion3

import dpla.ingestion3.enrichments.EnrichmentDriver
import dpla.ingestion3.mappers.providers.NaraExtractor
import dpla.ingestion3.model.DplaMapData
import dpla.ingestion3.utils.FlatFileIO
import org.apache.avro.Schema
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

object Integration {


  def main(args: Array[String]): Unit = {
    import com.databricks.spark.avro._
    val sparkConf: SparkConf = new SparkConf().setAppName("Nara").setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    implicit val dplaMapDataEncoder = org.apache.spark.sql.Encoders.kryo[DplaMapData]

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val harvestData = spark.read.avro(args(0))

    //val schemaString = new FlatFileIO().readFileAsString("/avro/MAPRecord.avsc")
    //val avroSchema = new Schema.Parser().parse(schemaString)

    val documents = harvestData.select("document").rdd
    val mappedData = documents.map((row: Row) => new NaraExtractor(row.getString(0)).build())
    mappedData.toDS().write.format("com.databricks.spark.avro").save(args(1)) //.option("avroSchema", schemaString)
    val roundTripMapped = spark.read.avro(args(1)).as[DplaMapData]
    val enrichedData = mappedData.map(record => new EnrichmentDriver().enrich(record))
    enrichedData.write.format("com.databricks.spark.avro").save(args(2))
    val roundTripEnriched = spark.read.avro(args(2)).as[DplaMapData]
    val jsonData = enrichedData.map(model.jsonlRecord).map(_.replaceAll("\n", " "))
    jsonData.saveAsTextFile(args(3))

    spark.stop()
  }
}
