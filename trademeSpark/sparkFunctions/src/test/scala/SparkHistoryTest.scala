import org.scalatest._


import java.util.HashMap
import scala.io.Source
import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import models.Listing


class SparkHistorySpec extends FlatSpec with BeforeAndAfter {

  private val master = "local"
  private val appName = "Listing-spark-testing"
  //private val checkpointDir = Files.createTempDirectory(appName).toString

  private var sc: SparkContext = _
  

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
    sc=null;
  }
  

  "buildHistory" should "build a transaction from listing" in {
    
    val transaction = HistoryListing.aggregateListing(sc, "/home/danie/development/trademeDev/trademeData/dailyTimeFiles_process/", "/tmp");

    //transaction.threeLevelCat shouldBe "level1|level2|level3"
    //transaction.mainCat shouldBe "level1"
  }

  
}
