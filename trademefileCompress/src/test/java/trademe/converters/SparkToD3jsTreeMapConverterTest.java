package trademe.converters;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import trademe.converters.spark.Transaction;

import com.google.gson.Gson;

public class SparkToD3jsTreeMapConverterTest {
	Gson gson = new Gson();
	
	
	private ConverterData testConverter(List<String> lines) {
		SparkToD3jsTreeMapConverter sparkToD3jsTreeMapConverter = new SparkToD3jsTreeMapConverter();
		return sparkToD3jsTreeMapConverter.convert(lines);
	}
	
	private String buildLine(String dayDate, Long amount, Long value, String[] cats) throws ParseException {
		String catStr = String.join("|", cats);
		Transaction transaction = new Transaction();
		transaction.setValue(value);
		transaction.setAmount(amount);
		transaction.setDayDate(dayDate);
		transaction.setCat(catStr);
		return gson.toJson(transaction);
	}

	@Test
	public void test1LevelCat() throws ParseException {
		String[] cat1 = {"cat1"};
		String[] cat2 = {"cat2"};
		String[] cat3 = {"cat3"};
		String dayDate = "20150823";
		
		List<String> testData = new ArrayList<String>();
		testData.add( buildLine(dayDate, 10L, 10L, cat1));
		testData.add( buildLine(dayDate, 11L, 12L, cat2));
		testData.add( buildLine(dayDate, 33L, 34L, cat3));
		
		ConverterData converterData = testConverter(testData);
		assertEquals(dayDate, converterData.getName());
		
		String expected = "{" +
							"\"name\":\"20150823\"," +
							"\"children\":"+
							"[" +
							"{\"name\":\"cat1\",\"children\":[],\"value\":10,\"amount\":10}," +
							"{\"name\":\"cat2\",\"children\":[],\"value\":12,\"amount\":11}," +
							"{\"name\":\"cat3\",\"children\":[],\"value\":34,\"amount\":33}" +
							"]" +
							"}";

		assertEquals(expected, converterData.getJSON());
	}
	
	@Test
	public void test3LevelCat() throws ParseException {
		String[] cat1 = {"cat1"};
		String[] cat2 = {"cat1","cat2"};
		String[] cat3 = {"cat1","cat2","cat3"};
		String dayDate = "20150823";
		
		List<String> testData = new ArrayList<String>();
		testData.add( buildLine(dayDate, 10L, 10L, cat1));
		testData.add( buildLine(dayDate, 11L, 12L, cat2));
		testData.add( buildLine(dayDate, 33L, 34L, cat3));
		
		ConverterData converterData = testConverter(testData);
		assertEquals(dayDate, converterData.getName());
		
		String expected = "{" +
							"\"name\":\"20150823\"," +
							"\"children\":"+
							"[" +
							"{\"name\":\"cat1\",\"children\":[" +
																"{\"name\":\"cat2\",\"children\":[" +
																									"{\"name\":\"cat3\",\"children\":[],\"value\":34,\"amount\":33}" +
																								"],\"value\":12,\"amount\":11}" +
															"],\"value\":10,\"amount\":10}" +
							
							"]" +
							"}";

		assertEquals(expected, converterData.getJSON());
	}
	
	@Test
	public void testLevelCat() throws ParseException {
		String[] cat1 = {"cat1"};
		String[] cat2 = {"cat1","cat2"};
		
		String dayDate = "20150823";
		
		List<String> testData = new ArrayList<String>();
		testData.add( buildLine(dayDate, 10L, 10L, cat1));
		testData.add( buildLine(dayDate, 11L, 12L, cat2));
		testData.add( buildLine(dayDate, 33L, 34L, cat2));
		
		ConverterData converterData = testConverter(testData);
		assertEquals(dayDate, converterData.getName());
		
		String expected = "{" +
							"\"name\":\"20150823\"," +
							"\"children\":"+
							"[" +
							"{\"name\":\"cat1\",\"children\":[" +
																"{\"name\":\"cat2\",\"children\":[],\"value\":12,\"amount\":11}," +
																"{\"name\":\"cat2\",\"children\":[],\"value\":34,\"amount\":33}" +
															"],\"value\":10,\"amount\":10}" +
							
							"]" +
							"}";

		assertEquals(expected, converterData.getJSON());
	}

}
