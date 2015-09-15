package trademe.model.processors;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import trademe.downloader.parser.LatestListingParser;
import trademe.downloaders.utils.Utils;
import trademe.model.Category;


public class LatestLising2FileProcessor<LatestListing> implements IProcessor<LatestListing>{	
	final static private Logger LOG = LogManager.getLogger(LatestLising2FileProcessor.class.getName());
	private Category cat;
	StringBuilder sb = new StringBuilder();
	public LatestLising2FileProcessor(Category cat) {
		this.cat = cat;
	}

	@Override
	public void process(LatestListing listing) {
		sb.append(listing);
		sb.append(";\n");
	}

	public void persist(DateTime date) {
		File htmlStorageFile = Utils.getLatestListingStorageFile(this.cat.getId(), date);
		try {
			Files.write(htmlStorageFile.toPath(), sb.toString().getBytes());
		} catch (IOException e) {
			LOG.error(e);
		}
}
	

}
