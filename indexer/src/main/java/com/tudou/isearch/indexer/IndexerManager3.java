package com.tudou.isearch.indexer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.tudou.isearch.common.Model;
import com.tudou.isearch.indexer.IFilterDirectoryReader.ISubReaderWrapper;

/**
 * 索引实例结点管理器<br>
 * 用于管理此结点上所有的索引实例(线程).
 * 
 * @author chenheng
 * 
 */
public class IndexerManager3<T extends Model> implements Manager<T> {
	private static final Logger logger = Logger
			.getLogger(IndexerManager3.class);

	/**
	 * 聚合索引文件路径
	 */
	private String assembledIndexPath;

	/**
	 * 已经注册的索引实例(线程)列表
	 */
	private List<Indexer<T>> ramIndexers;
	/**
	 * 聚合索引目录
	 */
	private Directory assembledDirectory;
	/**
	 * 聚合索引只读器
	 */
	private DirectoryReader assembledReader;
	/**
	 * 聚合索引写
	 */
	private IndexWriter assembledWriter;
	/**
	 * Lucene分析器
	 */
	private Analyzer analyzer;
	/**
	 * 标记待删除的索引记录
	 */
	private DeleteQueue assembledDeleteQueue;
	/**
	 * 单次删除term的最大数量为1000个.
	 */
	private final static long DELETE_TERM_THRESHOLD_ONE_TIME = 1000;
	/**
	 * 索引线程池
	 */
	private ThreadPoolTaskExecutor indexerPool;

	/**
	 * 启到计划调度器,用于控制将文件块合并到聚合文件中
	 */
	public void init() {
		logger.debug(">>>> [Assembled] init assembled directory & writer...");
		try {
			this.assembledDirectory = FSDirectory.open(Paths
					.get(this.assembledIndexPath));
			IndexWriterConfig config = new IndexWriterConfig(this.analyzer);
			this.assembledWriter = new IndexWriter(assembledDirectory, config);
		} catch (IOException e) {
			logger.error("!!!! [Assembled] IO Exception", e);
		}
		logger.debug(">>>> [Assembled] trigger indexer working...");
		for (Indexer<T> indexer : ramIndexers) {
			indexerPool.submit(indexer);
		}
	}

	/**
	 * 合并小文件块索引到聚合文件索引中
	 * 
	 * @throws IOException
	 */
	public synchronized void mergeSegmentIndexes() throws IOException {
		logger.debug(">>>> [Assembled] clean delete quene...");
		deleteAssembledIndexes(true);
		for (Indexer<T> indexer : ramIndexers) {
			logger.debug(">>>> [Assembled] begin to merge segment FS directories to assembled FS directory("
					+ assembledIndexPath + ")");
			Directory segmentDirectory = indexer.getSegmentDirectory();
			indexer.closeSegmentWriter();
			assembledWriter.addIndexes(segmentDirectory);
			assembledWriter.commit();
			indexer.reinit(false, true);
			logger.info(">>>> [Assembled] Segment FS directory has been merged to assembled FS directory("
					+ assembledIndexPath + ")");
			indexer.cleanSegmentIndex();
		}
	}

	public synchronized void deleteAssembledIndexes() throws IOException {
		deleteAssembledIndexes(false);
	}

	public synchronized void deleteAssembledIndexes(boolean clean)
			throws IOException {
		String uniqueKeyName = assembledDeleteQueue.getUniqueKeyName();
		LinkedBlockingQueue<String> queue = assembledDeleteQueue.getQueue();
		int i = 0;
		while (true) {
			String id = queue.poll();
			if (id == null) {
				break;
			}
			Term term = new Term(uniqueKeyName, id);
			assembledWriter.deleteDocuments(term);
			logger.info(">>>> [Assembled] Delete term(" + uniqueKeyName + ":"
					+ id + ") from assembled index.");
			++i;
			if (!clean && i >= DELETE_TERM_THRESHOLD_ONE_TIME) {
				break;
			}
		}
		logger.info(">>>> [Assembled] delete " + i
				+ " terms from assembled directory");
		assembledWriter.commit();
	}

	/**
	 * 注册索引实例(线程)到管理器中.
	 * 
	 * @param indexer
	 *            索引实例
	 */
	public void registerRAMIndexer(Indexer<T> indexer) {
		this.ramIndexers.add(indexer);
		indexer.setManager(this);
	}

	/**
	 * 当前所有索引实例(线程)的内存块目录
	 * 
	 * @return 当前所有索引实例(线程)的内存块目录
	 */
	public List<Directory> getAllRamDirectory() {
		List<Directory> ramDirectories = new LinkedList<Directory>();
		for (Indexer<T> indexer : ramIndexers) {
			if (indexer.getDirectory() != null) {
				ramDirectories.add(indexer.getDirectory());
			}
		}
		return ramDirectories;
	}

	/**
	 * 当前所有索引实例(线程)的文件块目录
	 * 
	 * @return 当前所有索引实例(线程)的文件块目录
	 */
	public List<Directory> getAllSegmentDirectory() {
		List<Directory> segments = new LinkedList<Directory>();
		for (Indexer<T> indexer : ramIndexers) {
			if (indexer.getSegmentDirectory() != null) {
				segments.add(indexer.getSegmentDirectory());
			}
		}
		return segments;
	}

	/**
	 * 包装当前的所有索引目录<br>
	 * 内存块目录,文件块上当,聚合文件目录
	 * 
	 * @return 内存块目录,文件块上当,聚合文件目录
	 */
	public DirectoryWrapper getDirectoryWrapper() {
		List<Directory> ramDirectories = getAllRamDirectory();
		List<Directory> segDirectories = getAllSegmentDirectory();
		DirectoryWrapper dw = new DirectoryWrapper(ramDirectories,
				segDirectories, assembledDirectory);
		return dw;
	}

	/**
	 * 当前所有索引实例的内存块只读器
	 * 
	 * @return
	 */
	public List<DirectoryReader> getAllRamReader() {
		List<DirectoryReader> ramReaders = new LinkedList<DirectoryReader>();
		for (Indexer<T> indexer : ramIndexers) {
			DirectoryReader reader = indexer.getReader();
			if (reader != null && reader.numDocs() > 0) {
				ramReaders.add(reader);
			}
		}
		return ramReaders;
	}

	/**
	 * 当前所有索引实例的文件块只读器
	 * 
	 * @return
	 */
	public List<DirectoryReader> getAllSegmentReader() {
		List<DirectoryReader> segmentReaders = new LinkedList<DirectoryReader>();
		for (Indexer<T> indexer : ramIndexers) {
			DirectoryReader reader = indexer.getSegmentReader();
			if (reader != null && reader.numDocs() > 0 ) {//
				segmentReaders.add(reader);
			}
		}
		return segmentReaders;
	}

	/**
	 * 包装只读器<br>
	 * 包括内存块只读器,文件块只读器,聚合文件只读器
	 * 
	 * @return
	 */
	public ReaderWrapper getReaderWrapper() {
		//List<DirectoryReader> ramReaders = getAllRamReader();
		List<DirectoryReader> segmentReaders = getAllSegmentReader();
		/*if (assembledDirectory != null) {
			try {
				if (assembledReader == null) {
					DirectoryReader dr = DirectoryReader
							.open(this.assembledDirectory);
					assembledReader = new IFilterDirectoryReader<T>(dr,
							new ISubReaderWrapper<T>(this.assembledDeleteQueue));
				} else {
					@SuppressWarnings("unchecked")
					DirectoryReader dr = ((IFilterDirectoryReader<T>) assembledReader)
							.doWrapDirectoryReader(assembledReader, assembledDeleteQueue);
					if (dr != null) {
						// don't close this reader because it maybe handled by
						// other
						// point-in-time threads which try to use it to read.
						// assembledReader.close();
						assembledReader = dr;
					}
				}
			} catch (IOException e) {
				logger.error("!!!! IO Exception", e);
			}
		}*/
		/*ReaderWrapper rw = new ReaderWrapper(ramReaders, segmentReaders,
				assembledReader);*/
		ReaderWrapper rw = new ReaderWrapper(null, segmentReaders,
		null);
		return rw;
	}

	public String getAssembledIndexPath() {
		return assembledIndexPath;
	}

	public void setAssembledIndexPath(String assembledIndexPath) {
		this.assembledIndexPath = assembledIndexPath;
	}

	public Analyzer getAnalyzer() {
		return analyzer;
	}

	public List<Indexer<T>> getRamIndexers() {
		return ramIndexers;
	}

	public void setRamIndexers(List<Indexer<T>> ramIndexers) {
		this.ramIndexers = ramIndexers;
	}

	public void setAnalyzer(Analyzer analyzer) {
		this.analyzer = analyzer;
	}

	public void setAssembledDeleteQueue(DeleteQueue assembledDeleteQueue) {
		this.assembledDeleteQueue = assembledDeleteQueue;
	}

	public DeleteQueue getAssembledDeleteQueue() {
		return assembledDeleteQueue;
	}

	public ThreadPoolTaskExecutor getIndexerPool() {
		return indexerPool;
	}

	public void setIndexerPool(ThreadPoolTaskExecutor indexerPool) {
		this.indexerPool = indexerPool;
	}

	@Override
	public void mergeManually() {
		for (Indexer<T> index : ramIndexers) {
			index.mergeManually();
		}
		logger.info(">>>> [Assembled] Manually merge all ram index to it's segment FS directory finished!");
	}
}
