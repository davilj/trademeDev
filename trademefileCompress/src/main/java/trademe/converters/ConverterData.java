package trademe.converters;

public class ConverterData {
	private String name;
	private String data;

	public ConverterData(String name, String data) {
		this.name = name;
		this.data = data;
	}

	public String getName() {
		return name;
	}

	public String getJSON() {
		return data;
	}

}
