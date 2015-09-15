package trademe.converters;

import java.util.List;

public interface IConverter {
	public ConverterData convert(List<String> lines);	
}