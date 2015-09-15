package trademe.downloaders.utils;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;

public class Utils {
	final static private SimpleDateFormat fileFormat = new SimpleDateFormat("YYYYMMddHHmm");
	final static private SimpleDateFormat dirFormat = new SimpleDateFormat("YYYYMMdd");
	static String PATH = "src/main/resources/";
	static final String LATESTLISTINGS = "LATESTLISTINGS/";
	static final String LISTING = "LISTING/";

	public static void setBasePath(String basePath) {
		PATH = basePath;
	}

	public static final File getFileName(String fileName) {
		return new File(PATH + fileName);
	}

	public static File[] getFiles(File path) {
		File dir = new File(PATH + File.separator + path.getAbsolutePath());
		if (!dir.exists()) {
			dir.mkdirs();
		}
		return dir.listFiles();
	}

	public static File getLatestListingStorageFile(String catId, DateTime date) {
		return new File(getDir("lastestListing", catId, date).getAbsoluteFile() + File.separator +fileFormat.format(date.toDate()) + ".ll");
	}
	
	public static File getLatestListingHTMLStorageFile(String catId, DateTime date, int index) {
		return new File(getDir("latestListingHTML", catId, date).getAbsoluteFile() + File.separator + fileFormat.format(date.toDate()) + "_" + Integer.toString(index) + ".llhtml");
	}

	private static File getDir(String storageDir, String catId, DateTime date) {
		String dayDir = dirFormat.format(date.toDate());
		File dir = new File(storageDir + File.separator + dayDir + File.separator + catId);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		return dir;
	}

	public static File addBasePath(File file) {
		File nwfile = new File(PATH + File.separator + file.getAbsolutePath());
		File dir = nwfile.getParentFile();
		if (!dir.exists()) {
			dir.mkdirs();
		}
		return nwfile;
	}

	public static class Date {
		static Pattern numPattern = Pattern.compile("\\d+");
		static Pattern secsPattern = Pattern.compile("\\d+ secs");
		static Pattern minPattern = Pattern.compile("\\d+ min");
		static Pattern hourPattern = Pattern.compile("\\d+ hour");

		public static DateTime parseTradeMeTimeLeft(String timeStr) {
			int sec = 0;
			sec += addTime(secsPattern, timeStr, 1);
			sec += addTime(minPattern, timeStr, 60);
			sec += addTime(hourPattern, timeStr, 60 * 60);
			return new DateTime(System.currentTimeMillis() + sec * 1000);
		}

		private static int addTime(Pattern pattern, String timeStr, int multiplier) {
			int num = 0;
			Matcher m = pattern.matcher(timeStr);
			while (m.find()) {
				Matcher numMatcher = numPattern.matcher(m.group());
				while (numMatcher.find()) {
					num = num + Integer.parseInt(numMatcher.group());
				}
			}
			return num * multiplier;
		}
	}

}
