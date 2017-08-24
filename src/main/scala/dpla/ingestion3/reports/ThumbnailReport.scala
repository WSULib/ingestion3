package dpla.ingestion3.reports

import dpla.ingestion3.model.{DplaMapData, EdmWebResource}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import java.net.URI

class ThumbnailReport (
                            val inputURI: String,
                            val outputURI: String,
                            val sparkMasterName: String,
                            val params: Array[String]) extends Report {

  override val sparkAppName: String = "ThumbnailReport"
  override def getInputURI: String = inputURI
  override def getOutputURI: String = outputURI
  override def getSparkMasterName: String = sparkMasterName
  override def getParams: Option[Array[String]] = {
    params.nonEmpty match {
      case true => Some(params)
      case _ => None
    }
  }

  /**
    * Process the incoming dataset (mapped or enriched records) and return a
    * DataFrame of computed results.
    *
    * @param ds    Dataset of DplaMapData (mapped or enriched records)
    * @param spark The Spark session, which contains encoding / parsing info.
    * @return DataFrame, typically of Row[value: String, count: Int]
    */
  override def process(ds: Dataset[DplaMapData], spark: SparkSession): DataFrame = {
    import spark.implicits._

    implicit val dplaMapDataEncoder =
      org.apache.spark.sql.Encoders.kryo[DplaMapData]

    val thumbnailData: Dataset[ThumbnailReportItem] = ds.map(dplaMapData => {

      val hasImageType = dplaMapData.sourceResource.`type`
        .filter{ _.toLowerCase() == "image"}.nonEmpty

      val providerUri = dplaMapData.edmWebResource.uri.toString
      val dplaUri = dplaMapData.oreAggregation.uri.toString

      val preview = dplaMapData.oreAggregation.preview match {
        case Some(preview) => Some(preview.uri.toString)
        case None => None
      }

      ThumbnailReportItem(hasImageType, preview, providerUri, dplaUri)
    })

    thumbnailData.select("hasImageType", "preview", "providerUri", "dplaUri")
      .filter("hasImageType = TRUE")
      .filter("preview is null")
  }
}

case class ThumbnailReportItem(hasImageType: Boolean,
                               preview: Option[String],
                               providerUri: String,
                               dplaUri: String)
