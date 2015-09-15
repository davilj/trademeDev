package trademe.converters.spark;

import java.util.Date;

public class Listing {
	private String dayDate;
	private String cat;
	private Long amount;
	
	public void setDayDate(String dayDate) {
		this.dayDate = dayDate;
	}
	public void setCat(String cat) {
		this.cat = cat;
	}
	public void setAmount(Long amount) {
		this.amount = amount;
	}
	public String getDayDate() {
		return dayDate;
	}
	public String getCat() {
		return cat;
	}
	public Long getAmount() {
		return amount;
	}
	
}
