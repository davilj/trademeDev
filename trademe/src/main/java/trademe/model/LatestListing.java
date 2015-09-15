package trademe.model;

public class LatestListing {
	private String title;
	protected String link;
	private String closingTimeText;
	private String bidInfo;
	private String priceInfo;

	public LatestListing(String title, String link, String closingTimeText, String bidInfo, String priceInfo) {
		this.title = title;
		this.link = link;
		this.closingTimeText = closingTimeText;
		this.bidInfo = bidInfo;
		this.priceInfo = priceInfo;
	}

	public String getPriceInfo() {
		return priceInfo;
	}

	public String getLink() {
		return link;
	}

	public String getClosingTimeText() {
		return closingTimeText;
	}
	
	public String getBidInfo() {
		return bidInfo;
	}

	public String getTitle() {
		return title;
	}

	@Override
	public String toString() {
		return "LatestListing [title=" + title + ", link=" + link + ", closingTimeText=" + closingTimeText + ", bidInfo=" + bidInfo + ", priceInfo="
				+ priceInfo + "]";
	}
}