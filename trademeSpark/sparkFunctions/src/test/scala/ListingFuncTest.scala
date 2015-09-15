import org.scalatest.FlatSpec
import java.text.SimpleDateFormat
import java.util.HashMap
import java.util.Date

//test method use by spark
class ListingFuncSpec extends FlatSpec {

  "buildListingM" should "build a listing from a string" in {
    val sdf = new SimpleDateFormat("yyyyMMddHHmm")
    val testData = "date=201508280242,title=this is the title, link=/building-renovation/tools/hand-tools/pliers/auction-936782526.htm, closingTimeText=closes in 2 mins, bidInfo=, priceInfo=$45.00;"
    val results = ListingFactory.buildListingMap(testData);
    assert(results != null)
    val listing = ListingFactory.buildListing(testData)
    assert(listing != null)
    //case class Listing(id: String, dayDate: Date, title: String, cat: String, link: String, priceInfo: Float, closingDate: Long, closingTimeText: String )
    assert("936782526"==listing.id)

    assert("20150828"==listing.dayDate)
    assert("201508280244"==sdf.format(new Date(listing.closingDate.toLong)))

    assert("this is the title"==listing.title)

    assert("/building-renovation/tools/hand-tools/pliers/auction-936782526.htm"==listing.link)
    assert("building-renovation|tools|hand-tools|pliers"==listing.cat)

    assert(false==listing.bid)
    assert(45.0==listing.priceInfo)
  }

  "checkBidding" should "return true if we have a value (not null)" in {
  	val map = new HashMap[String, String]();
  	map.put("bidInfo","tatatat")
  	assert(ListingFactory.checkBidding(map)==true)
  	val map2 = new HashMap[String, String]()
  	assert(ListingFactory.checkBidding(map2)==false)
  	map.put("bidInfo","")
  	assert(ListingFactory.checkBidding(map2)==false)
  }

}
