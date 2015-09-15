package trademe;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import trademe.downloader.parser.IParser;
import trademe.downloader.parser.LatestListingParser;
import trademe.downloaders.CategoryDownLoadService;
import trademe.downloaders.IDownLoadService;
import trademe.downloaders.LatestListingDownloadService;	
import trademe.model.Category;
import trademe.model.LatestListing;
import trademe.model.Listing;
import trademe.model.processors.IProcessor;
import trademe.model.processors.LatestLising2FileProcessor;
/**
 
 * @author danie
 *
 */
public class LatestListingController {
	private static Logger LOG = LogManager.getLogger(LatestListingController.class.getName());
	static Map<String, IDownLoadService<Listing>> mapString2ListingFac = new HashMap<>();
	
	public static void main(String[] args) {
		final Map<String, LatestListingDownloadService> mapCatId2LatestListingExtractor = new HashMap<>();

		IDownLoadService<Category> categoryDownloader = new CategoryDownLoadService();
		categoryDownloader.setProcessor(new IProcessor<Category>() {

			@Override
			public void process(Category category) {
				DateTime now = new DateTime();
				if (category.isMainCategory() && !isProperty(category) && !isServices(category) && !isJobs(category)) {
					LOG.debug("Process Category: " + category);
					
					LatestListingDownloadService le = mapCatId2LatestListingExtractor.get(category.getId());
					if (le == null) {
						le = new LatestListingDownloadService(category);
						IProcessor<LatestListing> processor = new LatestLising2FileProcessor<>(category);
						le.setProcessor(processor);
						IParser parser = new LatestListingParser();
						le.setParser(parser);
						mapCatId2LatestListingExtractor.put(category.getId(), le);
					}
					le.start(now);
					//TODO don't like this
					((LatestLising2FileProcessor)le.getProcessor()).persist(now);
				} else {
					LOG.debug("Not logging " + category.getName() + " since not a main category");
				}
			}

			private boolean isJobs(Category category) {
				return category.getName().toLowerCase().contains("jobs");
			}

			private boolean isProperty(Category category) {
				return category.getName().toLowerCase().contains("property");
			}
			
			private boolean isServices(Category category) {
				return category.getName().toLowerCase().contains("services");
			}
		});
		categoryDownloader.start(new DateTime());
	}

}
