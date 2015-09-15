import java.io.IOException;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import trademe.DailyFileFacotry;
import trademe.FileConvertor;
import trademe.converters.SparkToD3jsTreeMapConverter;
import trademe.converters.IConverter;
import trademe.downloaders.utils.Utils;

/**
 * 
 * @author danie
 * 
 *         Compress daily files over all cats and location, easier to use for
 *         spark
 */

public class TrademeFileMerger {
	private static Logger LOG = LogManager.getLogger(TrademeFileMerger.class.getName());

	public static void main(String[] args) {
		;
		if (args.length < 1) {
			LOG.error("USAGE: java -jar TrademeFileMerger.jar <basedir> <task> <taskParams>");
			System.exit(-1);
		}

		Utils.setBasePath(args[0]);

		switch (args[1]) {
		// merge files that is created every 14 minute, this run daily
		case "mergeDailyFile":
		case "mdf":
			LOG.info("Start file merger: " + new DateTime().toDate());
			long start = System.currentTimeMillis();
			String[] llargs = { args[2], args[3] };
			DailyFileFacotry.main(llargs);
			LOG.info("End file merger: " + new DateTime().toDate() + " -- " + (System.currentTimeMillis() - start) / 1000);
			break;

		case "buildD3jsFile":
		case "d3f":
			LOG.info("Start build d3js: " + new DateTime().toDate());
			long startD3f = System.currentTimeMillis();

			FileConvertor converter = new FileConvertor();
			IConverter spark2J3js = new SparkToD3jsTreeMapConverter();
			try {
				converter.convert(Paths.get(args[2].trim()), Paths.get(args[3].trim()), spark2J3js);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(-1);
			}
			LOG.info("End building d3js: " + new DateTime().toDate() + " -- " + (System.currentTimeMillis() - startD3f) / 1000);

			break;
		default:
			LOG.error("Can not parse command [" + args[1] + "]");
			System.exit(-1);
		}
	}
}
