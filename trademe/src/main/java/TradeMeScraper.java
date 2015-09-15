import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import trademe.LatestListingController;
import trademe.downloaders.utils.Utils;
/**
 * 
 * @author danie
 * 
 * Usage TradeMeScraper <basePath> <task> <taskParams>
 * 
 * <task> is one of the following:
 * latestListing
 * 		- <taskParam>
 * 				- period in minutes (get the latest listing that close in x minutes
 */

public class TradeMeScraper {
	private static Logger LOG = LogManager.getLogger(TradeMeScraper.class.getName());

	public static void main(String[] args) {
		;
		if (args.length<1) {
			LOG.error("USAGE: java -jar scraperDownloader.jar <task> <taskParams>");
			System.exit(-1);
		}
		
		Utils.setBasePath(args[0]);
		
		switch (args[1]) {
		case "latestListing":
		case "ll":
			//String[] llArgs = {args[2]};
			
			LOG.info("Start: " + new DateTime().toDate());
			long start = System.currentTimeMillis();
			String[] llargs = {};
			LatestListingController.main(llargs);
			LOG.info("End: " + new DateTime().toDate() + " -- " + (System.currentTimeMillis() - start)/1000);
			break;
		default:
			LOG.error("Can not parse command [" + args[1] + "]");
			System.exit(-1);
		}
	}
}
