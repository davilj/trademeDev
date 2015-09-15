package trademe.downloader.parser;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import trademe.model.LatestListing;

public class LatestListingParserTest {

	@Test
	public void testExtractMins() {
		LatestListingParser llp = new LatestListingParser();
		LatestListing ll = new LatestListing("","", "close in 3 mins","","");
		int min = llp.extractSeconds(ll);
		assertEquals(180, min);
	}
	
	@Test
	public void testExtractMinutes() {
		LatestListingParser llp = new LatestListingParser();
		LatestListing ll = new LatestListing("","", "close in 3 minutes","","");
		int min = llp.extractSeconds(ll);
		assertEquals(180, min);
	}
	
	@Test
	public void testExtractOneMinute() {
		LatestListingParser llp = new LatestListingParser();
		LatestListing ll = new LatestListing("","", "close in 1 minute","","");
		int min = llp.extractSeconds(ll);
		assertEquals(60, min);
	}
	
	@Test
	public void testExtractSeconds() {
		LatestListingParser llp = new LatestListingParser();
		LatestListing ll = new LatestListing("","", "close in 3 seconds","","");
		int sec = llp.extractSeconds(ll);
		assertEquals(3, sec);
	}
	
	@Test
	public void testExtractSecs() {
		LatestListingParser llp = new LatestListingParser();
		LatestListing ll = new LatestListing("","", "close in 3 secs","","");
		int sec = llp.extractSeconds(ll);
		assertEquals(3, sec);
	}
	
	@Test
	public void testExtractOneSec() {
		LatestListingParser llp = new LatestListingParser();
		LatestListing ll = new LatestListing("","", "close in 1 second","","");
		int sec = llp.extractSeconds(ll);
		assertEquals(1, sec);
	}
	
	@Test
	public void testExtractSpaces() {
		LatestListingParser llp = new LatestListingParser();
		LatestListing ll = new LatestListing("","", "close in 1 second","","");
		int sec = llp.extractSeconds(ll);
		assertEquals(1, sec);
		
		LatestListing ll2 = new LatestListing("","", "close in 1second","","");
		int sec2 = llp.extractSeconds(ll2);
		assertEquals(1, sec2);
		
		LatestListing ll3 = new LatestListing("","", "close in 1    second","","");
		int sec3 = llp.extractSeconds(ll3);
		assertEquals(1, sec3);
	}
	
	@Test
	public void testExtractEmpty() {
		LatestListingParser llp = new LatestListingParser();
		LatestListing ll = new LatestListing("","", "","","");
		int sec = llp.extractSeconds(ll);
		assertEquals(Integer.MAX_VALUE,sec);
	}
	
	@Test
	public void testExtracthour() {
		LatestListingParser llp = new LatestListingParser();
		LatestListing ll = new LatestListing("", "", "close in 1 hour","","");
		int sec = llp.extractSeconds(ll);
		assertEquals(Integer.MAX_VALUE,sec);
	}
}
