import org.scalatest._
import java.util.HashMap
import scala.io.Source
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import utils.FileUtils

//start a local copy of spark for unit testing
//this is NOT a test
class LocalSparkMultipleFiles extends FlatSpec with BeforeAndAfter {

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
  
  "Spark Program" should "return expected aggregation report" in {
    val resultDirAsStr = "/home/danie/development/trademeDev/trademeData/dailyTimeFiles_process"
    FileUtils.delete(new File(resultDirAsStr))
    ListingAggregator.buildListing(sc, "/home/danie/development/trademeDev/trademeData/dailyTimeFiles/latestListingMerge", resultDirAsStr)  
  }
}