package trademe.downloader.parser;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import trademe.model.LatestListing;

public interface IParser {

	public abstract LinkedList<LatestListing> parse(File latestListingFile);

	public abstract List<LatestListing> filterOnPeriod(LinkedList<LatestListing> listigs, int periodInSec);

}