package trademe;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import trademe.converters.SparkToD3jsTreeMapConverter;

public class FileCompressorTest {
	
	@Before
	public void setup() throws IOException {
		cleanDir((Paths.get("/home/danie/development/trademe/lastestListing")));
		cleanDir((Paths.get("/home/danie/development/trademefileCompress/src/test/resources/results")));
		CopyOption[] options = new CopyOption[]{
			      StandardCopyOption.REPLACE_EXISTING,
			      StandardCopyOption.COPY_ATTRIBUTES
			    }; 
			copyDir(Paths.get("/home/danie/development/trademe/lastestListing_backup"), Paths.get("/home/danie/development/trademe/lastestListing") );
	}
	
	@Test
	public void testD3Generating() throws IOException {
		FileConvertor converter = new FileConvertor();
		Path from = Paths.get("/home/danie/development/trademefileCompress/src/test/resources/transactionsBy3Cat.json");
		Path to = Paths.get("/home/danie/development/trademefileCompress/src/test/resources/results");
		converter.convert(from, to, new SparkToD3jsTreeMapConverter());
	}

	@Test
	public void testMerge() throws IOException {
		
		DailyFileFacotry fileCompressor = new DailyFileFacotry();
		String toPath = "/home/danie/tmp/trademe"; 
		cleanDir((Paths.get(toPath)));
		fileCompressor.buildFileFromDirectories(toPath	, "/home/danie/development/trademe/lastestListing");
		
		int numberOfLines = 0;
		for (Path entry : Files.newDirectoryStream(Paths.get(toPath))) {
			numberOfLines += Files.readAllLines(entry).size();
		}
		assertEquals(32976, numberOfLines);
	}
	
	@Test
	public void testMergeExistingFile() throws IOException {
		
		DailyFileFacotry fileCompressor = new DailyFileFacotry();
		String toPath = "/home/danie/tmp/trademe"; 
		cleanDir((Paths.get(toPath)));
		fileCompressor.buildFileFromDirectories(toPath	, "/home/danie/development/trademe/lastestListing");
		
		int numberOfLines = 0;
		for (Path entry : Files.newDirectoryStream(Paths.get(toPath))) {
			numberOfLines += Files.readAllLines(entry).size();
		}
		assertEquals(32976, numberOfLines);
		
		//run merge again...it should merge files
		setup();
		fileCompressor.buildFileFromDirectories(toPath	, "/home/danie/development/trademe/lastestListing");
		Set<Path> results = Files.list(Paths.get(toPath)).collect(Collectors.toSet());
		assertEquals(3, results.size());
		numberOfLines = 0;
		for (Path entry : Files.newDirectoryStream(Paths.get(toPath))) {
			numberOfLines += Files.readAllLines(entry).size();
		}
		assertEquals(32976*2, numberOfLines);
	}
	
	private void cleanDir(Path dir) throws IOException {
		if (!Files.exists(dir)) return;
		for (Path path : Files.newDirectoryStream(dir)) {
			if (Files.isDirectory(path)) {
				cleanDir(path);
			} else {
				Files.delete(path);
			}
		}
		Files.delete(dir);
	}
	
	private void copyDir(Path from, Path to) throws IOException {
		if (Files.exists(to)) {
			cleanDir(to);
		}
		org.apache.commons.io.FileUtils.copyDirectory(from.toFile(), to.toFile());
	}
}
