package com.tudou.isearch.indexer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.LinkedBlockingQueue;

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
public class RAMIndexer<T extends Model> extends AbstractIndexer<T> {
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
	 * 内存索引块上限常量值
	 */
	private final static long RAM_THRESHOLD = 256 * 1024 * 1024;// 256MB

	private static final Logger logger = Logger.getLogger(RAMIndexer.class);

	public RAMIndexer(String id, Analyzer analyzer,
			LinkedBlockingQueue<T> queue, String segmentPath, Monitor monitor)
			throws IOException {
		this(id, analyzer, queue, RAM_THRESHOLD, segmentPath, monitor);
	}

	public RAMIndexer(String id, Analyzer analyzer,
			LinkedBlockingQueue<T> queue, long ramThreshold,
			String segmentPath, Monitor monitor) throws IOException {
		super(monitor);
		this.analyzer = analyzer;
		this.queue = queue;
		this.ramThreshold = ramThreshold;
		this.segmentPath = segmentPath;
		this.segmentDirectory = FSDirectory.open(Paths.get(this.segmentPath +"/" + id));
		IndexWriterConfig conf = new IndexWriterConfig(this.analyzer);
		this.segmentWriter = new IndexWriter(segmentDirectory, conf);
		init();
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
					// 建立索引前先删除,以防重复(严进宽出)
					iManager.deleteIndex(content);
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
			segmentReader = DirectoryReader.open(segmentDirectory);
		} else {
			DirectoryReader dr = DirectoryReader.openIfChanged(segmentReader);
			if (dr != null) {
				segmentReader.close();
				segmentReader = dr;
			}
		}
	}

	public void delete(Model model) throws IOException {
		String uniqueKeyName = model.getUniqueKeyName();
		if (uniqueKeyName != null && !uniqueKeyName.trim().equals("")) {
			Term term = new Term(uniqueKeyName, model.getUniqueKeyValue());
			iwriter.deleteDocuments(term);
			iwriter.commit();
			logger.info(">>>> Delete term(" + model.getUniqueKeyName() + ": "
					+ model.getUniqueKeyValue() + ") from ram index.");
			segmentWriter.deleteDocuments(term);
			segmentWriter.commit();
			logger.info(">>>> Delete term(" + model.getUniqueKeyName() + ": "
					+ model.getUniqueKeyValue() + ") from segment index.");
		}
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
				DirectoryReader dr = DirectoryReader
						.openIfChanged(segmentReader);
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
	
	public void clearSegmentIndex() throws IOException {
		this.segmentWriter.deleteAll();
		this.segmentWriter.commit();
	}
}
