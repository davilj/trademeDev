package trademe.downloader.parser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import trademe.model.LatestListing;
import trademe.model.Listing;
	/**
 * Do a search of the latest listing in the Category specified
 * @author danie
 *
 */
public class LatestListingParser implements IParser  {
	final static private Logger LOG = LogManager.getLogger(LatestListingParser.class.getName());
	 Pattern minutesText = Pattern.compile("\\d+\\s*min");
	 Pattern secondText = Pattern.compile("\\d+\\s*sec");
	 Pattern number = Pattern.compile("\\d+");
	//http://www.trademe.co.nz/Browse/CategoryListings.aspx?mcatpath=antiques-collectables&v=List
	
	final static private String listingInCategoryTemplate = "http://www.trademe.co.nz/Browse/CategoryListings.aspx?sort_order=expiry_asc&mcatpath=%s&v=List"; 
	//http://www.trademe.co.nz/browse/categorylistings.aspx?v=list&rptpath=187-&mcatpath=antiques-collectables&page=1&sort_order=expiry_asc
	final static private String listingInCategoryTemplatePaged = "http://www.trademe.co.nz/Browse/CategoryListings.aspx?page=%s&sort_order=expiry_asc&mcatpath=%s&v=List"; 
		
	/* (non-Javadoc)
	 * @see trademe.downloader.parser.Parser#parse(java.io.File)
	 */
	@Override
	public LinkedList<LatestListing> parse(File file) {
		LinkedList<LatestListing> listings = new LinkedList<>();
		try {
			Document doc = Jsoup.parse(file, "UTF-8", "");
			for (Element listingCard : doc.select("li.listingCard")) {
				Elements links = listingCard.select("a[href]");
				String href = links.iterator().next().attr("href");
				Elements closings = listingCard.select("div.listingCloseDateTime");
				String endTime="";
				if (closings.size() > 0) {
					Element closingSoon = closings.iterator().next();
					endTime = closingSoon.text().toLowerCase();
				}
				Elements title = listingCard.select("div.listingTitle");
				String titleInfo = "";
				if (!title.isEmpty()) {
					Element titleElement = title.iterator().next();
					titleInfo=titleElement.text();
				}
				
				Elements price = listingCard.select("div.listingBidPrice");
				String priceInfo = "";
				if (!price.isEmpty()) {
					Element priceElement = price.iterator().next();
					priceInfo=priceElement.text();
				}
				
				Elements bids = listingCard.select("div.listingNumberOfBidsText");
				String bidInfo="";
				if (bids.size() > 0) {
					Element closingElement = bids.iterator().next();
					bidInfo=closingElement.text();
				}
				listings.add(new LatestListing(titleInfo, href, endTime, bidInfo, priceInfo));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return listings;
	}

	@Override
	public List<LatestListing> filterOnPeriod(LinkedList<LatestListing> all, int periodInSec) {
		List<LatestListing> filtered = all
				.parallelStream()
				.filter(listing ->  (! (extractSeconds(listing) > periodInSec)))
				.collect(Collectors.toList());
		return filtered;
	}
	
	protected int extractSeconds(LatestListing latestListing) {
		int seconds = Integer.MAX_VALUE;
		String time = latestListing.getClosingTimeText();
		if (time.equals("auction closed")) {
			return 1;
		}
		
		Matcher secondTxt = secondText.matcher(time);
		if (secondTxt.find()) {
			seconds = extractNumber(secondTxt.group());
		} else {
			Matcher minTxt = minutesText.matcher(time);
			if (minTxt.find()) {
				seconds = extractNumber(minTxt.group()) * 60;
			} else {
				LOG.info("Could not map [" + time + "]");
				seconds = Integer.MAX_VALUE;
			}
		}
 		return seconds;
	}
	
	private int extractNumber(String numberText) {
		Matcher matcher =number.matcher(numberText);
		if (matcher.find()) {
			return Integer.parseInt(matcher.group());
		} else {
			return Integer.MAX_VALUE;
		}
	}

}
