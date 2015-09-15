package trademe.downloaders;

import org.joda.time.DateTime;

import trademe.downloader.parser.IParser;
import trademe.model.processors.IProcessor;

public abstract class AbstractDownLoadService<M> implements IDownLoadService<M> {
	protected IProcessor<M> processor;
	protected IParser parser;

	public AbstractDownLoadService() {
		super();
	}

	public abstract void start(DateTime now);

	public void setProcessor(IProcessor<M> processor) {
		this.processor = processor;
	}

	public void setParser(IParser parser) {
		this.parser = parser;
	}

	public IProcessor<M> getProcessor() {
		return processor;
	}

	public IParser getParser() {
		return parser;
	}
}