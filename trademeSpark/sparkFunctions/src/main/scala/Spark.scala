import utils.LogHelper
import utils.FileUtils
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

import models._
/**
*Analytics on listings
**/
object ListingAggregator extends LogHelper {

	def getListOfFiles(dir: String):List[File] = {
  		val d = new File(dir)
  		if (d.exists && d.isDirectory) {
    		d.listFiles.filter(_.isFile).toList
  		} else {
    		List[File]()
  		}
	}

	def buildTransaction(listing: Listing):Transaction = {
		val catArray = listing.cat.split("\\|")
		val threeLevelCat = if (catArray.length>2) {
			catArray.slice(0,3).mkString("|")
		} else {
			catArray.mkString("|")
		}
		Transaction(listing.id, listing.dayDate, catArray(0), threeLevelCat, listing.priceInfo)
	}

	case class ListingByCat(id: String, dayDate: String, cat:String)

	def buildSQLReport(dataFrame : DataFrame, reportName: String, resultDirAsStr: String): Unit = {
		val byMainCatMultiple = Paths.get(resultDirAsStr,reportName + ".multiple.json")
		val byMainCat = Paths.get(resultDirAsStr, reportName + ".json")
		val transactionsByMainCat  =dataFrame.write.format("json").save(byMainCatMultiple.toFile.getAbsolutePath)
		FileUtils.mergeMultiParts(byMainCatMultiple, byMainCat)
	}

	def parseFile(sc: SparkContext, filename : File, resultDirAsStr : String):Unit = {
		logger.info("Ready to extract listing from: " + filename)
		//wrap in SQL to allow sql queries
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._


		//TODO remove duplicates...using reduceByKey and reduce again
		val listingsRDD = sc.textFile(filename.getAbsolutePath()).map(line => ListingFactory.buildListing(line))
		val transactionRDD = listingsRDD.filter(listing => listing.bid==true).map(buildTransaction)
		
		//convert to dataframes
		val listingDF = listingsRDD.map(listing => {
				val cats = listing.cat.split("\\|")
				ListingByCat(listing.id, listing.dayDate, cats(0))
			}).toDF()
		listingDF.registerTempTable("listingByCat")
		val transactionDF = transactionRDD.toDF()
		transactionDF.registerTempTable("transaction")

		//listings
		val listingByMainCat = sqlContext.sql("SELECT dayDate, cat, COUNT(*) as amount FROM listingByCat GROUP BY dayDate, cat ORDER BY cat").
								select("dayDate","cat","amount")
		buildSQLReport(listingByMainCat,"listingsByMainCat", resultDirAsStr);

		//transactions
		val transactionsByMainCat  = sqlContext.sql("SELECT dayDate, mainCat as cat, COUNT(*) as amount, SUM(priceInfo) value FROM transaction GROUP BY dayDate, mainCat ORDER BY cat").
								select("dayDate","cat","amount","value")
		buildSQLReport(transactionsByMainCat,"transactionsByMainCat", resultDirAsStr);
		
		//FileUtils.mergeMultiParts(byMainCatMultiple, byMainCat)
		val transactionsBy3Cat = sqlContext.sql("SELECT dayDate, threeLevelCat as cat, COUNT(*) as amount, SUM(priceInfo) as value FROM transaction GROUP BY dayDate, threeLevelCat ORDER BY cat").
								select("dayDate", "cat", "amount", "value")
		buildSQLReport(transactionsBy3Cat,"transactionsBy3Cat", resultDirAsStr);
	}

	def buildListing(sc: SparkContext, listingDirAsStr : String, resultDirAsStr : String) = {
		val files = getListOfFiles(listingDirAsStr)
		logger.info("Number files of to process: " + files.size)
		val resultDir = new File(resultDirAsStr)
		if (!Files.exists(resultDir.toPath())) {
			Files.createDirectory(resultDir.toPath())
		}

	    val reportData = files.map(file => parseFile(sc, file, resultDirAsStr))
	}

	def main(args: Array[String]):Unit = {
	    val listingDirAsStr = args(0)
	    logger.info("Listing dir: " + listingDirAsStr)
	    val resultDirAsStr = args(1)
	    logger.info("ResultDir dir: " + resultDirAsStr)
	    val conf = new SparkConf().setAppName("Listing factory")
	    val sc = new SparkContext(conf)
	    buildListing(sc, listingDirAsStr, resultDirAsStr)
	}
}