import utils.LogHelper
import utils.FileUtils
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.ArrayList

import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import models._
/**
*Read daily listing and transaction file by cat and build files for listing/transaction for last x days
**/
object HistoryListing extends LogHelper {

	def getListOfFiles(dir: String):List[File] = {
  		val d = new File(dir)
  		logger.info("dir: " + d)

  		if (d.exists && d.isDirectory) {
    		d.listFiles.filter(_.isFile).toList
  		} else {
  			logger.info("no files")
    		List[File]()
  		}
	}

	def buildSQLReport(dataFrame : DataFrame, reportName: String, resultDirAsStr: String): Unit = {
		val byMainCatMultiple = Paths.get(resultDirAsStr,reportName + ".multiple.json")
		val byMainCat = Paths.get(resultDirAsStr, reportName + ".json")
		val cache = collection.mutable.Map[String, ArrayList[Row]]()

		//ensure we have everything on the master (we know we can handle the dataset), add were clause on day before collect
		val dataframe  = dataFrame.collect()
		dataframe.foreach(line => {
			val cat = line.getString(1)
			val entries = cache.getOrElseUpdate(cat, new ArrayList[Row]())
			entries.add(line)
			cache(cat)=entries

		})

		println(cache)

		//.write.format("json").save(byMainCatMultiple.toFile.getAbsolutePath)
		FileUtils.mergeMultiParts(byMainCatMultiple, byMainCat)
	}

	def aggregateListing(sc: SparkContext, listingDirAsStr : String, resultDirAsStr : String) = {
		
		logger.info("#######################################################################################")
		logger.info("Load all transactions in: " + listingDirAsStr)

		val resultDir = new File(resultDirAsStr)
		if (!Files.exists(resultDir.toPath())) {
			Files.createDirectory(resultDir.toPath())
		}
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	    val transactionsDF = sqlContext.read.json(listingDirAsStr+File.separator+"*_transactionsByMainCat.json")
	    transactionsDF.registerTempTable("transactions")
	    val transactionsHistoryByCat = sqlContext.sql("SELECT dayDate, cat, sum(amount) as amount, sum(value) as value FROM transactions GROUP BY cat, dayDate ORDER BY cat,dayDate DESC")
	    val subsetDF = transactionsHistoryByCat.select(transactionsHistoryByCat("dayDate"), transactionsHistoryByCat("cat"), transactionsHistoryByCat("amount"), transactionsHistoryByCat("value"))
		buildSQLReport(subsetDF, "history", "/tmp")

	}

	def main(args: Array[String]):Unit = {
	    val listingDirAsStr = args(0)
	    logger.info("Listing dir: " + listingDirAsStr)
	    val resultDirAsStr = args(1)
	    logger.info("ResultDir dir: " + resultDirAsStr)
	    val conf = new SparkConf().setAppName("Listing factory")
	    val sc = new SparkContext(conf)
	    aggregateListing(sc, listingDirAsStr, resultDirAsStr)
	}
}