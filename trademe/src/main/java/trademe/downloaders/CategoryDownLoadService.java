package trademe.downloaders;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.json.Json;
import javax.json.stream.JsonParser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import trademe.downloaders.utils.Downloader;
import trademe.downloaders.utils.Utils;
import trademe.model.Category;
import trademe.model.processors.IProcessor;
/**
 * Load the AllCategories file...if the file is not found download it from trademe (using trademe rest api)
 * @author danie
 *
 */
public class CategoryDownLoadService extends AbstractDownLoadService<Category> implements
		IDownLoadService<Category> {
	private static final Logger LOG = LogManager.getLogger(CategoryDownLoadService.class.getName());	
	static final String FILE_NAME = "AllCategories.json";
	private String startURL = "http://api.trademe.co.nz/v1/Categories.json";

	

	void loadFromNetwork() {
		File allCats = Utils.getFileName(FILE_NAME);
		Downloader.download(startURL, allCats);
	}

	public void start(DateTime now) {
			File allCats = Utils.getFileName(FILE_NAME);
			if (!allCats.exists()) {
				LOG.info("Did not find loacal Category file, download...: " + allCats);
				loadFromNetwork();
			}
			LOG.info("Load category from file: " + allCats);
			loadFromFile(allCats, processor);
	}

	public boolean loadFromFile(File file, IProcessor<Category> processor) {
		try {
			InputStream is = new FileInputStream(file);
			JsonParser parser = Json.createParser(is);
			Category current = null;
			String currentProp = "";
			while (parser.hasNext()) {
				JsonParser.Event event = parser.next();
				switch (event) {
				// case START_ARRAY:
				// case END_ARRAY:
				// case START_OBJECT:
				// case END_OBJECT:
				// case VALUE_FALSE:
				// System.out.println("bValue: " + false);
				// break;
				// case VALUE_NULL:
				// case VALUE_TRUE:
				// System.out.println("bValue: " + true);
				// break;
				case KEY_NAME:
					currentProp = parser.getString();
	
					break;
				case VALUE_STRING:
					String value = parser.getString();
					if (current == null) {
						current = new Category();
					}
					current.setProp(currentProp, value);	
					if (currentProp.equals(Category.PATH)) {
						if (processor != null) {
							processor.process(current);
						}
						current = null;
					}
					break;
				// case VALUE_NUMBER:
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return true;
	}
}
