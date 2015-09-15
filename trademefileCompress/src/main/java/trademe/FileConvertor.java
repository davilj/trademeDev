package trademe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import trademe.converters.ConverterData;
import trademe.converters.IConverter;

public class FileConvertor {
	public void convert(Path from, Path to, IConverter converter) throws IOException {
		if (!Files.exists(to)) {
			Files.createDirectories(to);
		}
		
		ConverterData converterData = converter.convert(Files.readAllLines(from));
		String result = String.join("\n", converterData.getJSON());
		
		Files.write(Paths.get(to.toFile().getAbsolutePath(), converterData.getName()+ ".json"), result.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
	}
}
