package dpla.ingestion3.harvesters.api

import org.apache.avro.Schema
import org.apache.spark.sql.DataFrame
import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.file.NaraFileHarvestMain._
import dpla.ingestion3.utils.FlatFileIO
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.log4j.Logger

import scala.util.Try


abstract class ApiHarvester(shortName: String,
                            conf: i3Conf,
                            outputDir: String,
                            harvestLogger: Logger)
  extends Harvester(shortName, conf, outputDir, harvestLogger) {

  // Abstract method queryParams should set base query parameters for API call.
  protected val queryParams: Map[String, String]

  // Abstract method doHarvest should execute the harvest and save (@see saveOut)
  protected def localApiHarvest: Unit

  // Schema for harvested data.
  protected val schema: Schema = {
    val schemaStr = new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc")
    new Schema.Parser().parse(schemaStr)
  }

  /**
    * This is lazy b/c queryParams should be printed before avroWriter is set.
    * @see doHarvest
    */
  protected lazy val avroWriter: DataFileWriter[GenericRecord] =
  getAvroWriter(outputFile, schema)

  /**
    * Saves the records
    *
    * @param docs - List of ApiRecords to save out
    */
  protected def saveOut(docs: List[ApiRecord]): Unit = {

    docs.foreach(doc => {
      val startTime = System.currentTimeMillis()
      val unixEpoch = startTime / 1000L

      val genericRecord = new GenericData.Record(schema)

      genericRecord.put("id", doc.id)
      genericRecord.put("ingestDate", unixEpoch)
      genericRecord.put("provider", shortName)
      genericRecord.put("document", doc.document)
      genericRecord.put("mimetype", mimeType)
      avroWriter.append(genericRecord)
    })
  }

  /**
    * Generalized driver for ApiHarvesters invokes localApiHarvest() method and reports
    * summary information.
    */
  protected def runHarvest: Try[DataFrame] = Try{

    avroWriter.setFlushOnEveryBlock(true)

    // Calls the local implementation
    localApiHarvest

    avroWriter.close()

    spark.read.avro(outputDir)
  }
}