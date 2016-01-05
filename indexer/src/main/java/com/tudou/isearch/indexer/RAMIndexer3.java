package com.tudou.isearch.indexer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.LinkedBlockingQueue;
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

import com.tudou.isearch.common.Model;
import com.tudou.isearch.indexer.IFilterDirectoryReader.ISubReaderWrapper;

/**
 * 内存索引处理器<br>
 * 
 * 消费特定类型T的数据，并为之建立索引
 * 
 * @author chenheng
 * 
 * @param <T>
 */
public class RAMIndexer3<T extends Model> extends AbstractIndexer<T> {
	/**
	 * 分析器
	 */
	private Analyzer analyzer;
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
	 * 文件块索引路径<br>
	 * 当内存索引达到上限值时，会dump到此路径下生成文件块索引
	 */
	private String segmentPath;
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
	private DeleteQueue deleteQueue;
	/**
	 * 标记聚合文件块内待删除的索引记录
	 */
	private DeleteQueue assembledDeleteQueue;
	/**
	 * 待索引数据Queue
	 */
	private LinkedBlockingQueue<T> modelQueue;
	/**
	 * 单次删除term的最大数量为1000个.
	 */
	private final static long DELETE_TERM_THRESHOLD_ONE_TIME = 1000;

	private static final Logger logger = Logger.getLogger(RAMIndexer3.class);

	/**
	 * 初始化内存索引块空间<br>
	 * 初始化内存写索引器<br>
	 * 
	 * @throws IOException
	 */
	public void init() throws IOException {
		logger.debug(">>>> init RAM stuff...");
		this.directory = new RAMDirectory();
		IndexWriterConfig ramConf = new IndexWriterConfig(this.analyzer);
		this.iwriter = new IndexWriter(directory, ramConf);
		this.readOnly = false;
		logger.debug(">>>> init sgement stuff...");
		this.segmentDirectory = FSDirectory.open(Paths.get(this.segmentPath));
		IndexWriterConfig segConf = new IndexWriterConfig(this.analyzer);
		this.segmentWriter = new IndexWriter(segmentDirectory, segConf);
		try {
			// try to open segment reader in case it's already existed.
			DirectoryReader dr = DirectoryReader.open(segmentDirectory);
			segmentReader = new IFilterDirectoryReader(dr,
					this.deleteQueue);
		} catch (IOException e) { 
			logger.error("!!!! IO Exception", e);
		}
	}

	public void reinit() throws IOException {
		reinit(true, false);
	}

	/**
	 * 重建内存索引块空间及内存写索引器<br>
	 * 
	 * @param initRam
	 *            重置内存
	 * @param initFs
	 *            重置文件块
	 * @throws IOException
	 */
	public void reinit(boolean initRam, boolean initFs) throws IOException {
		if (initRam) {
			logger.debug(">>>> reinit RAM stuff...");
			this.directory = new RAMDirectory();
			IndexWriterConfig ramConf = new IndexWriterConfig(this.analyzer);
			this.iwriter = new IndexWriter(directory, ramConf);
			this.reader = null;
			this.readOnly = false;
		}
		if (initFs) {
			logger.debug(">>>> reinit FS stuff...");
			IndexWriterConfig segConf = new IndexWriterConfig(this.analyzer);
			this.segmentWriter = new IndexWriter(segmentDirectory, segConf);
		}
	}

	public synchronized boolean closeSegmentWriter() {
		try {
			this.segmentWriter.close();
			return true;
		} catch (IOException e) {
			logger.error("!!!! IO Exception", e);
		}
		return false;
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
					if (modelQueue != null) {
						T content = modelQueue.poll(2, TimeUnit.SECONDS);
						if (content == null) {
							continue;
						}
						deleteQueue.add(content);
						assembledDeleteQueue.add(content);
						buildDocument(content);
					}
				}
			} catch (Exception e) {
				logger.error("!!!! exception!", e);
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
	private synchronized void merge2SingleFSDirectory() throws IOException {
		logger.debug(">>>> clean delete quene...");
		deleteSegmentIndexes(true);
		logger.debug(">>>> begin to merge RAM directory to FS directory("
				+ segmentPath + ")");
		segmentWriter.addIndexes(directory);
		segmentWriter.commit();
		logger.info(">>>> RAM directory has been merged to FS directory("
				+ segmentPath + ")");
		reinit();
		if (segmentReader == null) {
			DirectoryReader dr = DirectoryReader.open(segmentDirectory);
			segmentReader = new IFilterDirectoryReader(dr,
					this.deleteQueue);
		}
	}

	public synchronized void deleteSegmentIndexes() throws IOException {
		deleteSegmentIndexes(false);
	}

	/**
	 * 清除文件块内删除队列中的所有索引数据
	 * 
	 * @param clean
	 * <br>
	 *            true 全部清除;<br>
	 *            false 每次最多删除1000(DELETE_TERM_THRESHOLD_ONE_TIME)条<br>
	 * 
	 * @throws IOException
	 */
	public synchronized void deleteSegmentIndexes(boolean clean)
			throws IOException {
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
			logger.debug(">>>> Delete term(" + uniqueKeyName + ":" + id
					+ ") from segment index.");
			++i;
			if (!clean && i >= DELETE_TERM_THRESHOLD_ONE_TIME) {
				break;
			}
		}
		logger.info(">>>> delete " + i + " terms from segment directory");
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
					// don't close this reader because it maybe handled by other
					// point-in-time threads which try to use it to read.
					// reader.close();
					reader = dr;
				}
			}
		} catch (IOException e) {
			// logger.debug("!!!! IO Exception", e);
		}
		return reader;
	}

	public DirectoryReader getSegmentReader() {
		try {
			if (segmentReader != null) {
				@SuppressWarnings("unchecked")
				DirectoryReader dr = ((IFilterDirectoryReader) segmentReader)
						.doWrapDirectoryReader(segmentReader, deleteQueue);
				if (dr != null) {
					// don't close this reader because it maybe handled by other
					// point-in-time threads which try to use it to read.
					// segmentReader.close();
					segmentReader = dr;
				}
			}
		} catch (IOException e) {
			logger.error("!!!! IO Exception", e);
		}
		return segmentReader;
	}

	@Override
	public void cleanSegmentIndex() throws IOException {
		logger.debug(">>>> clean up full segment index...");
		this.segmentWriter.deleteAll();
		this.segmentWriter.commit();
	}

	public Analyzer getAnalyzer() {
		return analyzer;
	}

	public void setAnalyzer(Analyzer analyzer) {
		this.analyzer = analyzer;
	}

	public IndexWriter getIwriter() {
		return iwriter;
	}

	public void setIwriter(IndexWriter iwriter) {
		this.iwriter = iwriter;
	}

	public String getSegmentPath() {
		return segmentPath;
	}

	public void setSegmentPath(String segmentPath) {
		this.segmentPath = segmentPath;
	}

	public long getRamThreshold() {
		return ramThreshold;
	}

	public void setRamThreshold(long ramThreshold) {
		this.ramThreshold = ramThreshold;
	}

	public boolean isReadOnly() {
		return readOnly;
	}

	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}

	public DeleteQueue getDeleteQueue() {
		return deleteQueue;
	}

	public void setDeleteQueue(DeleteQueue deleteQueue) {
		this.deleteQueue = deleteQueue;
	}

	public DeleteQueue getAssembledDeleteQueue() {
		return assembledDeleteQueue;
	}

	public void setAssembledDeleteQueue(DeleteQueue assembledDeleteQueue) {
		this.assembledDeleteQueue = assembledDeleteQueue;
	}

	public void setDirectory(RAMDirectory directory) {
		this.directory = directory;
	}

	public void setReader(DirectoryReader reader) {
		this.reader = reader;
	}

	public void setSegmentDirectory(Directory segmentDirectory) {
		this.segmentDirectory = segmentDirectory;
	}

	public void setSegmentWriter(IndexWriter segmentWriter) {
		this.segmentWriter = segmentWriter;
	}

	public void setSegmentReader(DirectoryReader segmentReader) {
		this.segmentReader = segmentReader;
	}

	public LinkedBlockingQueue<T> getModelQueue() {
		return modelQueue;
	}

	public void setModelQueue(LinkedBlockingQueue<T> modelQueue) {
		this.modelQueue = modelQueue;
	}

	@Override
	public void mergeManually() {
		logger.info(">>>> merge ram directory to FS directory manually...");
		// do nothing except mark read only is true, and then it's will trigger
		// merge auto.
		this.readOnly = true;
		try {
			// close ram writer, otherwise, it will raise
			// LockObtainFailedException
			this.iwriter.close();
		} catch (IOException e) {
			logger.error("!!!! IO Exception", e);
		}
	}

}
