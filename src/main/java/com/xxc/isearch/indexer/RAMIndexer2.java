package com.tudou.isearch.indexer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

import com.tudou.isearch.Model;
import com.tudou.isearch.indexer.IFilterDirectoryReader.ISubReaderWrapper;
import com.tudou.isearch.monitor.Monitor;

/**
 * 内存索引处理器<br>
 * 建立索引前先尝试删除索引,即严进宽出.
 * 
 * 消费特定类型T的数据，并为之建立索引
 * 
 * @author chenheng
 * 
 * @param <T>
 */
public class RAMIndexer2<T extends Model> extends AbstractIndexer<T> {
	/**
	 * 分析器
	 */
	private final Analyzer analyzer;
	/**
	 * 内存读写目录
	 */
	private RAMDirectory directory;
	/**
	 * 读索引
	 */
	private DirectoryReader reader;
	/**
	 * 写索引
	 */
	private IndexWriter iwriter;
	/**
	 * 待索引数据队列
	 */
	private final LinkedBlockingQueue<T> queue;
	/**
	 * 文件块索引路径<br>
	 * 当内存索引达到上限值时，会dump到此路径下生成文件块索引
	 */
	private final String segmentPath;
	/**
	 * 内存索引块上限<br>
	 * 当内存索引达到上限值时，会dump到此路径下生成文件块索引
	 */
	private long ramThreshold;
	/**
	 * 只读开关，当且仅当内存块索引达到上限时为true(只读，不在处理索引新增/修改/删除请求);<br>
	 * 内存块dump为文件块后，重新标记为false.
	 */
	private boolean readOnly;
	/**
	 * 文件块索引目录队列
	 */
	private Directory segmentDirectory;
	/**
	 * 文件块写索引器队列
	 */
	private IndexWriter segmentWriter;
	/**
	 * 文件块读索引队列
	 */
	private DirectoryReader segmentReader;
	/**
	 * 标记文件块内待删除的索引记录
	 */
	private DeleteQueue<T> deleteQueue;
	/**
	 * 标记聚合文件块内待删除的索引记录
	 */
	private DeleteQueue<T> assembledDeleteQueue;
	/**
	 * 内存索引块上限常量值
	 */
	private final static long RAM_THRESHOLD = 256 * 1024 * 1024;// 256MB
	/**
	 * 单次删除term的最大数量为1000个.
	 */
	private final static long DELETE_TERM_THRESHOLD_ONE_TIME = 1000;

	private static final Logger logger = Logger.getLogger(RAMIndexer2.class);

	public RAMIndexer2(String id, Analyzer analyzer,
			LinkedBlockingQueue<T> queue, String segmentPath, Monitor monitor,
			DeleteQueue<T> assembledDeleteQueue) throws IOException {
		this(id, analyzer, queue, RAM_THRESHOLD, segmentPath, monitor,
				assembledDeleteQueue);
	}

	public RAMIndexer2(String id, Analyzer analyzer,
			LinkedBlockingQueue<T> queue, long ramThreshold,
			String segmentPath, Monitor monitor,
			DeleteQueue<T> assembledDeleteQueue) throws IOException {
		super(monitor);
		this.analyzer = analyzer;
		this.queue = queue;
		this.ramThreshold = ramThreshold;
		this.segmentPath = segmentPath;
		this.segmentDirectory = FSDirectory.open(Paths.get(this.segmentPath
				+ "/" + id));
		IndexWriterConfig conf = new IndexWriterConfig(this.analyzer);
		this.segmentWriter = new IndexWriter(segmentDirectory, conf);
		this.deleteQueue = new DeleteQueue<T>();
		this.assembledDeleteQueue = assembledDeleteQueue;
		init();
		schedule();
	}

	/**
	 * 初始化内存索引块空间<br>
	 * 初始化内存写索引器<br>
	 * 
	 * @throws IOException
	 */
	public void init() throws IOException {
		logger.debug(">>>> init RAM Indexer instance...");
		this.directory = new RAMDirectory();
		IndexWriterConfig conf = new IndexWriterConfig(this.analyzer);
		this.iwriter = new IndexWriter(directory, conf);
		this.readOnly = false;
	}

	/**
	 * 重建内存索引块空间及内存写索引器<br>
	 * 
	 * @throws IOException
	 */
	public void reinit() throws IOException {
		init();
	}

	private void schedule() {
		/**
		 * 计划调度器,用于控制将文件块合并到聚合文件中
		 */
		ScheduledExecutorService pool = Executors
				.newSingleThreadScheduledExecutor();
		pool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					deleteSegmentIndex();
				} catch (IOException e) {
					logger.error("!!!! IOException", e);
					e.printStackTrace();
				}
			}
		}, 1, 3, TimeUnit.HOURS);
	}

	@Override
	public void consume() {
		while (true) {
			try {
				if (this.readOnly) {
					// 只读标识打开时，dump内存块索引到文件块中
					// dump2FSDirectory();
					merge2SingleFSDirectory();
				} else {
					// 消耗数据并建立索引
					T content;
					content = queue.take();
					deleteQueue.add(content);
					assembledDeleteQueue.add(content);
					buildDocument(content);
				}
			} catch (InterruptedException e) {
				logger.error("!!!! interrupt exception!", e);
			} catch (IOException e) {
				logger.error("!!!! io exception!", e);
			}
		}

	}

	private void buildDocument(T content) throws IOException {
		logger.debug(">>>> begin to consume content(" + content + ")...");
		Document doc = new Document();
		content.toDocument(doc);
		String id = content.getUniqueKeyName();
		if (id == null) {
			this.iwriter.addDocument(doc);
			this.iwriter.commit();
		} else {
			Term idTerm = new Term(id, content.getUniqueKeyValue());
			this.iwriter.updateDocument(idTerm, doc);
			this.iwriter.commit();
		}
		this.monitor.consumeEvent();
		logger.debug(">>>> content(" + content + ") consumed");
		if (this.directory.ramBytesUsed() >= this.ramThreshold) {
			logger.info(">>>> RAM directory is full now and pending to merge to file system directory...");
			this.iwriter.close();
			this.readOnly = true;
		}
	}

	/**
	 * 只dump内存块到单一文件块中.
	 * 
	 * @throws IOException
	 */
	private void merge2SingleFSDirectory() throws IOException {
		logger.info(">>>> begin to merge RAM directory to FS directory("
				+ segmentPath + ")");
		segmentWriter.addIndexes(directory);
		segmentWriter.commit();
		logger.info(">>>> RAM directory has been merged to FS directory");
		reinit();
		if (segmentReader == null) {
			DirectoryReader dr = DirectoryReader.open(segmentDirectory);
			segmentReader = new IFilterDirectoryReader<T>(dr,
					new ISubReaderWrapper<T>(this.deleteQueue));
		}
	}

	private void deleteSegmentIndex() throws IOException {
		String uniqueKeyName = deleteQueue.getUniqueKeyName();
		LinkedBlockingQueue<String> queue = deleteQueue.getQueue();
		int i = 0;
		while (true) {
			String id = queue.poll();
			if (id == null) {
				break;
			}
			Term term = new Term(uniqueKeyName, id);
			segmentWriter.deleteDocuments(term);
			logger.info(">>>> Delete term(" + uniqueKeyName + ":" + id
					+ ") from segment index.");
			if (++i >= DELETE_TERM_THRESHOLD_ONE_TIME) {
				break;
			}
		}
		segmentWriter.commit();
	}

	public RAMDirectory getDirectory() {
		return this.directory;
	}

	public Directory getSegmentDirectory() {
		return segmentDirectory;
	}

	public IndexWriter getSegmentWriter() {
		return segmentWriter;
	}

	public DirectoryReader getReader() {
		try {
			if (reader == null) {
				reader = DirectoryReader.open(directory);
			} else {
				DirectoryReader dr = DirectoryReader.openIfChanged(reader);
				if (dr != null) {
					reader.close();
					reader = dr;
				}
			}
		} catch (IOException e) {
			logger.error("!!!! IO Exception", e);
		}
		return reader;
	}

	public DirectoryReader getSegmentReader() {
		try {
			if (segmentReader != null) {
				@SuppressWarnings("unchecked")
				DirectoryReader dr = ((IFilterDirectoryReader<T>) segmentReader)
						.doWrapDirectoryReader(segmentReader, deleteQueue);
				if (dr != null) {
					segmentReader.close();
					segmentReader = dr;
				}
			}
		} catch (IOException e) {
			logger.error("!!!! IO Exception", e);
		}
		return segmentReader;
	}

	@Override
	public void clearSegmentIndex() throws IOException {
		this.segmentWriter.deleteAll();
		this.segmentWriter.commit();
	}
}
