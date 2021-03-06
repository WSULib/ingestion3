package dpla.ingestion3.harvesters.oai.refactor

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class AllSetsOaiRelationTest extends FlatSpec with SharedSparkContext {

  private val oaiConfiguration = OaiConfiguration(Map("verb" -> "ListRecords"))

  private val oaiMethods = new OaiMethods with Serializable {

    override def parsePageIntoRecords(pageEither: Either[OaiError, OaiPage], removeDeleted: Boolean) = Seq(
      Right(OaiRecord("a", "document", Seq()))
    )

    override def listAllRecordPages() =  ???

    override def listAllSetPages() = Seq(
      Right(OaiPage("1")),
      Right(OaiPage("2"))
    )

    override def listAllRecordPagesForSet(setEither: Either[OaiError, OaiSet]) = listAllSetPages()

    override def parsePageIntoSets(pageEither: Either[OaiError, OaiPage]) = Seq(
      Right(OaiSet("1", "")), Right(OaiSet("2", ""))
    )
  }

  private lazy val sqlContext = SparkSession.builder().getOrCreate().sqlContext
  private lazy val relation = new AllSetsOaiRelation(oaiConfiguration, oaiMethods)(sqlContext)


  "An AllSetsOaiRelation" should "build a scan using OaiMethods" in {
    val rdd = relation.buildScan()
    assert(rdd.count === 8)
  }
}
