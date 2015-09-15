package trademe.downloaders.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Download a url to a file
 * 
 * @author danie
 *
 */
public class Downloader {
	private static final Logger LOG = LogManager.getLogger(Downloader.class
			.getName());

	public static void download(String fromUrl, File toFile) {
		LOG.debug("Downloading: " + fromUrl + " --> " + toFile);
		URL website;
		try {
			website = new URL(fromUrl);
			ReadableByteChannel rbc = Channels.newChannel(website.openStream());
			FileOutputStream fos = new FileOutputStream(toFile);
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
		} catch (IOException e) {
			LOG.error("Error downloading: " + fromUrl + " --> " + toFile, e);
			throw new RuntimeException(e);
		}
	}
}
