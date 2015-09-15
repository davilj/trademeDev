package trademe.downloaders;

import org.joda.time.DateTime;

import trademe.downloader.parser.IParser;
import trademe.model.processors.IProcessor;

public interface IDownLoadService<M> {	

	public abstract void start(DateTime date);

	public abstract void setProcessor(IProcessor<M> processor);
	
	public abstract void setParser(IParser parser);
	
}