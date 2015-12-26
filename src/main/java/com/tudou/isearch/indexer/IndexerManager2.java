package com.tudou.isearch.indexer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.tudou.isearch.Model;
import com.tudou.isearch.indexer.IFilterDirectoryReader.ISubReaderWrapper;

/**
 * 索引实例结点管理器<br>
 * 用于管理此结点上所有的索引实例(线程).
 * 
 * @author chenheng
 * 
 */
public class IndexerManager2<T extends Model> implements Manager<T> {
	private static final Logger logger = Logger
			.getLogger(IndexerManager2.class);

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
	 * 计划调度器,用于控制将文件块合并到聚合文件中
	 */
	private ScheduledExecutorService pool;
	/**
	 * Lucene分析器
	 */
	private final Analyzer analyzer;
	/**
	 * 标记待删除的索引记录
	 */
	private DeleteQueue<T> assembledDeleteQueue;
	/**
	 * 单次删除term的最大数量为1000个.
	 */
	private final static long DELETE_TERM_THRESHOLD_ONE_TIME = 1000;

	/**
	 * 索引实例结点管理器<br>
	 * 
	 * @param assembledIndexPath
	 *            聚合文件索引路径
	 * @param analyzer
	 *            lucene分析器
	 */
	public IndexerManager2(String assembledIndexPath, Analyzer analyzer) {
		this.assembledIndexPath = assembledIndexPath;
		this.analyzer = analyzer;
		this.ramIndexers = new LinkedList<Indexer<T>>();
		this.assembledDeleteQueue = new DeleteQueue<T>();
		if (assembledIndexPath != null) {
			try {
				this.assembledDirectory = FSDirectory.open(Paths
						.get(this.assembledIndexPath));
				IndexWriterConfig config = new IndexWriterConfig(this.analyzer);
				this.assembledWriter = new IndexWriter(assembledDirectory,
						config);
			} catch (IOException e) {
				logger.error("!!!! IO Exception", e);
			}
		}
		init();
		schedule();
	}

	/**
	 * 启到计划调度器,用于控制将文件块合并到聚合文件中
	 */
	private void init() {
		pool = Executors.newSingleThreadScheduledExecutor();
		pool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					mergeSegmentIndexes();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}, 1, 30, TimeUnit.HOURS);
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
					deleteAssembledIndex();
				} catch (IOException e) {
					logger.error("!!!! IOException", e);
					e.printStackTrace();
				}
			}
		}, 1, 3, TimeUnit.HOURS);
	}

	/**
	 * 合并小文件块索引到聚合文件索引中
	 * 
	 * @throws IOException
	 */
	private void mergeSegmentIndexes() throws IOException {
		for (Indexer<T> indexer : ramIndexers) {
			Directory segmentDirectory = indexer.getSegmentDirectory();
			assembledWriter.addIndexes(segmentDirectory);
			assembledWriter.commit();
			indexer.clearSegmentIndex();
		}
	}

	private void deleteAssembledIndex() throws IOException {
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
			logger.info(">>>> Delete term(" + uniqueKeyName + ":" + id
					+ ") from assembled index.");
			if (++i >= DELETE_TERM_THRESHOLD_ONE_TIME) {
				break;
			}
		}
		assembledWriter.commit();
	}

	public void deleteIndex(Model model) throws IOException {
		// do nothing
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
			if (reader != null) {
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
			if (indexer.getSegmentReader() != null) {
				segmentReaders.add(indexer.getSegmentReader());
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
		List<DirectoryReader> ramReaders = getAllRamReader();
		List<DirectoryReader> segmentReaders = getAllSegmentReader();
		if (assembledDirectory != null) {
			try {
				if (assembledReader == null) {
					DirectoryReader dr = DirectoryReader
							.open(this.assembledDirectory);
					assembledReader = new IFilterDirectoryReader<T>(dr,
							new ISubReaderWrapper<T>(this.assembledDeleteQueue));
				} else {
					DirectoryReader dr = DirectoryReader
							.openIfChanged(assembledReader);
					if (dr != null) {
						assembledReader.close();
						assembledReader = dr;
					}
				}
			} catch (IOException e) {
				logger.error("!!!! IO Exception", e);
			}
		}
		ReaderWrapper rw = new ReaderWrapper(ramReaders, segmentReaders,
				assembledReader);
		return rw;
	}

	public DeleteQueue<T> getAssembledDeleteQueue() {
		return assembledDeleteQueue;
	}
}
