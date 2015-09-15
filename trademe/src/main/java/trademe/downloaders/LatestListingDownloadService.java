package trademe.downloaders;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;

import javax.json.Json;
import javax.json.stream.JsonParser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import trademe.downloader.parser.IParser;
import trademe.downloaders.utils.Downloader;
import trademe.downloaders.utils.Utils;
import trademe.model.Category;
import trademe.model.LatestListing;
import trademe.model.Listing;
import trademe.model.processors.IProcessor;
import trademe.model.processors.LatestLising2FileProcessor;

public class LatestListingDownloadService extends AbstractDownLoadService<LatestListing> implements IDownLoadService<LatestListing> {
	private static final Logger LOG = LogManager.getLogger(LatestListingDownloadService.class.getName());
	
	// http://www.trademe.co.nz/Browse/CategoryListings.aspx?sort_order=expiry_asc&mcatpath=%2Ftrade-me-motors%2Fcar-parts-accessories%2Fvintage-parts%2Fgear-boxes&v=List
	final static private String listingInCategoryTemplate = "http://www.trademe.co.nz/Browse/CategoryListings.aspx?sort_order=expiry_asc&mcatpath=%s&v=List";
	// http://www.trademe.co.nz/browse/categorylistings.aspx?v=list&mcatpath=gaming&page=2&sort_order=expiry_asc&rptpath=202-
	final static private String listingInCategoryTemplatePaged = "http://www.trademe.co.nz/Browse/CategoryListings.aspx?v=list&mcatpath=%s&page=%d&sort_order=expiry_asc";

	private Category cat;
	private String catPath;
	private String listingInCat;

	public LatestListingDownloadService(Category cat) {

		try {
			this.cat = cat;
			this.catPath = URLEncoder.encode(cat.getPath(), "UTF-8");
			listingInCat = String.format(listingInCategoryTemplate, catPath);

		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void start(DateTime now)  {
		List<LatestListing> latestListing = new LinkedList<>();
		int index = 0;
		boolean allListingWithInPeriond = true;
		boolean haveResults2Save = true;
		int period = 15 * 60;
		do {
			index++;
			File htmlStorageFile = Utils.getLatestListingHTMLStorageFile(this.cat.getId(), now, index);
			if (!htmlStorageFile.exists()) {
				if (index == 1) {
					Downloader.download(listingInCat, htmlStorageFile);
				} else {
					String paged = String.format(listingInCategoryTemplatePaged, catPath, index);
					Downloader.download(paged, htmlStorageFile);
				}
			}
			LinkedList<LatestListing> listings = this.parser.parse(htmlStorageFile);
			

			List<LatestListing> filtered = this.parser.filterOnPeriod(listings, period);

			allListingWithInPeriond = (filtered.size() - listings.size()) == 0;
			
			LOG.info("Download [" + this.cat.getName() + "] " + listings.size() + " latest listings");
			LOG.info("Adding [" + this.cat.getName() + "] "  + filtered.size() + " latest listings");
				
			haveResults2Save = filtered.size() != 0;
			for (LatestListing listing : filtered) {
				this.processor.process(listing);
			}
			LOG.info("allListingWithInPeriond [" + allListingWithInPeriond + "] haveResults2Save + ["  + haveResults2Save + "]");
		} while (allListingWithInPeriond && haveResults2Save);
	}

	public boolean loadFromFile(File file, IProcessor<Listing> processor) {
		try {
			InputStream is = new FileInputStream(file);
			JsonParser parser = Json.createParser(is);
			Listing current = null;
			String currentProp = "";
			while (parser.hasNext()) {
				JsonParser.Event event = parser.next();
				switch (event) {
				// case START_ARRAY:
				// case END_ARRAY:
				// case START_OBJECT:
				// case END_OBJECT:
				// case VALUE_FALSE:
				// System.out.println("bValue: " + false);
				// break;
				// case VALUE_NULL:
				// case VALUE_TRUE:
				// System.out.println("bValue: " + true);
				// break;
				case KEY_NAME:
					currentProp = parser.getString();

					break;
				case VALUE_STRING:
					String value = parser.getString();
					if (current == null) {
						current = new Listing();
					}
					current.setProp(currentProp, value);
					if (currentProp.equals("")) {
						if (processor != null) {
							processor.process(current);
						}
						current = null;
					}
					break;
				// case VALUE_NUMBER:
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return true;
	}

	

}
