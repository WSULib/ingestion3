package dpla.ingestion3.reports
import dpla.ingestion3.model.DplaMapData
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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

  }
}
