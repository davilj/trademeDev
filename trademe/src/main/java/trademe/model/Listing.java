package trademe.model;

import org.joda.time.DateTime;

import trademe.downloaders.utils.Utils;

public class Listing  {
	public static final String LINK = "link";
	public static final String TITLE = "title";
	public static final String LOCATION = "location";
	public static final String CLOSEDATE = "closedate";
	public static final String BID_PRICE = "bid_price";
	public static final String BID_TEXT = "bid_text";
	public static final String RESERVE = "reserve";
	public static final String DATE = "date";
	
	String id;
	String link;
	String title;
	String bid;
	String bidText;
	String reserve;
	String dateText;
	String location;
	DateTime endDate;
	Category cat;
	
	public DateTime getEndDate() {
		return this.endDate;
	}
	
	public void setCat(Category cat) {
		this.cat = cat;
	}
	
	public Category getCat() {
		return this.cat;
	}
	
	public void setProp(String name, Object value) {
		switch (name) {
		case LINK:
			link = (String)value;
			id = extractId(link);
			break;
			
		case TITLE:
			title = (String)value;
			break;
			
		case LOCATION:
			location = (String)value;
			break;
			
		case CLOSEDATE:
			endDate = new DateTime(Utils.Date.parseTradeMeTimeLeft((String)value));
			break;
			
		case BID_PRICE:
			bid = (String)value;
			break;
			
		case BID_TEXT:
			bidText = (String)value;
			break;
			
		case RESERVE:
			reserve = (String)value;
			break;
			
		case DATE:
			dateText = (String) value;
			break;
			
		default:
			break;
		}
	}
	
	private String extractId(String link2) {
		if (link2.equals("")) return link2;
		int endIndex = link.indexOf(".htm");
		int index = link2.lastIndexOf("-")+1;
		return link2.substring(index, endIndex);
	}

	public boolean doWeHaveAbid() {
		return this.bid!=null || !bid.equals("");
	}

	@Override
	public String toString() {
		return "Listing [id=" + id + ", title=" + title + ", link=" + link
				+ ", bid=" + bid + ", bidText=" + bidText + ", reserve="
				+ reserve + ", dateText=" + dateText + ", location=" + location
				+ ", endDate=" + endDate + ", cat=" + cat + "]";
	}
	
}
