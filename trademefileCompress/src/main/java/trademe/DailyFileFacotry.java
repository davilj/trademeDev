package trademe;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Merge the files generated every 15 minutes into one big daily file
 * @author danie
 *
 */
public class DailyFileFacotry {
	private static Logger LOG = LogManager.getLogger(DailyFileFacotry.class.getName());
	static SimpleDateFormat sdf = new SimpleDateFormat("YYYYmmdd");
	
	/*
	 * walk over daily dir and build daily files
	 */
	public void buildFileFromDirectories(String outputDir, String inputDir) {
		Path out = Paths.get(outputDir);

		try {
			if (!Files.exists(out)) {
				Files.createDirectory(out);
			}
			Path in = Paths.get(inputDir);
			for (Path entry : Files.newDirectoryStream(in)) {
				// expecting dir as dates
				List<Path> files2Delete = handleDateDirs(entry, out);
				removeFiles(files2Delete);
			}
			// FileUtils.deleteDirectory(in.toFile());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private List<Path> handleDateDirs(Path dateDir, Path outPutDir) throws ParseException, IOException {
		String dirNameAsStr = dateDir.getFileName().toFile().getName();
		// check if we work with a dir that is a date
		Date dateOfDir = sdf.parse(dirNameAsStr);
		Path newFile = getValidFileName(Paths.get(outPutDir.toFile().getAbsolutePath(), dirNameAsStr));
		List<Path> files2Delete = new ArrayList<Path>();
		LOG.info("Creating new file: " + newFile);

		try (OutputStream fos = new BufferedOutputStream(Files.newOutputStream(newFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND))) {
			StringBuilder builder = new StringBuilder();
			for (Path catDir : Files.newDirectoryStream(dateDir)) {
				for (Path dataFile : Files.newDirectoryStream(catDir)) {
					for (String line : Files.readAllLines(dataFile)) {
						String newLine = line.replace("]", "").replace("LatestListing [",
								"date=" + dataFile.getFileName().toFile().getName().replace(".ll", "") + ",");
						newLine = newLine + "\n";
						fos.write(newLine.getBytes());
					}
					LOG.info("\tAdded: " + dataFile);	
					files2Delete.add(dataFile);
				}
			}
		} catch (IOException io) {
			throw new RuntimeException(io);
		}

		return files2Delete;
	}

	private Path getValidFileName(Path path) {
		return path;
		/*
		 * int counter = 1; Path tmp = path; while (Files.exists(tmp)) { tmp =
		 * Paths.get(tmp.toFile().getAbsolutePath() + "_" + counter); counter++;
		 * } return tmp;
		 */
	}

	private void removeFiles(List<Path> dataFiles) throws IOException {
		for (Path path : dataFiles) {
			Files.deleteIfExists(path);
		}
	}

	public static void main(String[] args) {
		String from = args[0];
		String toDir = args[1];
		DailyFileFacotry fileCompressor = new DailyFileFacotry();
		fileCompressor.buildFileFromDirectories(from, toDir);
	}

}
