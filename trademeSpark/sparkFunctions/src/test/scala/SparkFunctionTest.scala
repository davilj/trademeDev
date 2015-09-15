import org.scalatest._


import java.util.HashMap
import scala.io.Source
import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import models.Listing


class SparkSpec extends FlatSpec with Matchers {
  def testForCat(catName:String, expectedValue:Float) = {
    def readFileContent(s:String):Float = {
      val lines = Source.fromFile(s).getLines.toList
      val results = lines
          .filter(_.startsWith("(" + catName))
          .map(line => {
              val tokens = line.split(",") 
              tokens(1).toFloat
              
      })

      val reduced = if (results.size > 0) {
        //lets try and reduce
        val total =  results.reduce((a,b) => a + b)
        total
      } else {
        println("[" +s + "] has no " + catName)
        0
      }
      reduced
    }
    val location = "/home/danie/development/trademeSpark/sparkFunctions/src/test/resources/20150828AggreGateByCatValueStr"
    val files = new File(location).listFiles().filter(_.isFile).filter(_.getName().startsWith("part")).toList
    val totalHomeLiving = files.map(file => readFileContent(file.getAbsolutePath())).reduce((a,b) => a+b)

    assert(totalHomeLiving==expectedValue)
  }

  "Spark" should "match my results" in {
    testForCat("home-living",182684.58F)
    testForCat("clothing-fashion",47249.855F)
  }

  "buildTransactionWith4Levels" should "build a transaction from listing" in {
    val listing = Listing("1","20120823","title","level1|level2|level3|level4", "link", true, 0.0F, 1, "still open")
    val transaction = ListingAggregator.buildTransaction(listing);

    transaction.threeLevelCat shouldBe "level1|level2|level3"
    transaction.mainCat shouldBe "level1"
  }

  "buildTransactionWith3Levels" should "build a transaction from listing" in {
    val listing = Listing("1","20120823","title","level1|level2|level3", "link", true, 0.0F, 1, "still open")
    val transaction = ListingAggregator.buildTransaction(listing);

    transaction.threeLevelCat shouldBe "level1|level2|level3"
    transaction.mainCat shouldBe "level1"
  }

  "buildTransactionWith2Levels" should "build a transaction from listing" in {
    val listing = Listing("1","20120823","title","level1|level2", "link", true, 0.0F, 1, "still open")
    val transaction = ListingAggregator.buildTransaction(listing);

    transaction.threeLevelCat shouldBe "level1|level2"
    transaction.mainCat shouldBe "level1"
  }

  "buildTransactionWith1Levels" should "build a transaction from listing" in {
    val listing = Listing("1","20120823","title","level1", "link", true, 0.0F, 1, "still open")
    val transaction = ListingAggregator.buildTransaction(listing);

    transaction.threeLevelCat shouldBe "level1"
    transaction.mainCat shouldBe "level1"
  }

}
