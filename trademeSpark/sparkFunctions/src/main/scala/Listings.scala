import java.util.Date
import java.text.SimpleDateFormat
import java.util.HashMap
import java.util.regex.Pattern
import java.util.regex.Matcher


import utils.LogHelper
import models.Listing

/**
*Conver a string (line from daily file) into a Listing
**/
object ListingFactory extends LogHelper {
	
	val minutePattern = Pattern.compile("\\d+\\s*min");
	val secondPattern = Pattern.compile("\\d+\\s*sec");
	val numberPattern = Pattern.compile("\\d+");
	val moneyPattern = Pattern.compile("\\d+\\.\\d{2}")

	def checkBidding(map:HashMap[String, String]):Boolean = {
		val result = map.get("bidInfo")
		!(result==null || result.equals(""))
	}

	def extractMoneyValue(s:String):Double = {
		val matcher = moneyPattern.matcher(s)
		if (matcher.find()) {
			matcher.group().toDouble
		} else {
			0.0
		}
	}

	def extractNumber(s: String):Int = {
		val numberStr = numberPattern.matcher(s)
	    val number = if (numberStr.find()) {
	    	numberStr.group().toInt
	    } else {
	    	//should never happen
	    	0
	    }
	    number
	}

	def buildListing(s: String):Listing = {
		val map = buildListingMap(s)
		//println(map.get("date") + " --> " + map.get("closingDate"))
		val dayDate = map.get("date")
		val closingDate =map.get("closingDate").toLong
		val priceInfo = map.get("priceInfo").toDouble
		val bid = if (map.get("bidInfo")=="") {
			false
		} else {
			true
		}
		
		//TODO there is a lot of data we do not use...do we need all of this in listing
		Listing(map.get("id"), dayDate, map.get("title"),  map.get("cat"), map.get("link"), bid, priceInfo, closingDate, map.get("closingTimeText"))
	}


	//parse line, return a map with all the props of a listing
	def buildListingMap(s: String):HashMap[String, String] = {
		val sdf = new SimpleDateFormat("yyyyMMddHHmm")
		val dayDate = new SimpleDateFormat("yyyyMMdd")
		val dataMap = new HashMap[String, String]
	  	//try {
	    val tokens = s.split(",").map(x => x.trim())
	    var date = new Date()
	    tokens.map(x => {
	    	val keyValue = x.split("=").map(_.trim())
	    	val key = keyValue(0)

	    	val value = if (keyValue.length==2) {
	    		keyValue(1)
	    	} else {
	    		""
	    	}

	    	val updatedValue = key match {
	    		case "date" => {
	    						
	    						date = sdf.parse(value.trim())
	    						dayDate.format(date)
	    					}
	    		case "priceInfo" => {
	    							"" + extractMoneyValue(value)
	    						}
	    		case "link" => {
	    						//build cat
	    						val tokens = value.split("/")
	    						val idStr = tokens(tokens.length-1).replace(".htm","").replace("auction-","")
	    						dataMap.put("id", idStr)
	    						val catStr = tokens.dropRight(1).drop(1).mkString("|")
	    						dataMap.put("cat", catStr)
	    						dataMap.put("mainCat",tokens(1))
	    						value
	    					}
	    		case "closingTimeText" => {
	    						val seconds = secondPattern.matcher(value)
	    						val delta = if (seconds.find()) {
	    							extractNumber(seconds.group) * 1000
	    						} else {
	    							val minText = minutePattern.matcher(value)
	    							if (minText.find()) {
	    								extractNumber(minText.group) * 60000
	    							} else {
	    								0
	    							}
	    						}
	    						val closingDate = date.getTime() + delta
	    						dataMap.put("closingDate", "" + closingDate)

	    						value
	    					}
	    					
	    		case _ => value

	    	}
			dataMap.put(key,updatedValue)
		})
		//} catch {
		//	case e: Exception => logger.error("Could not parse: " + s, e)
		//}
	    
	    return dataMap
	  }
}
