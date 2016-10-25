package com.tudou.isearch;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;

import com.tudou.isearch.indexer.DeleteQueue;
import com.tudou.isearch.indexer.DirectoryWrapper;
import com.tudou.isearch.indexer.ReaderWrapper;
import com.tudou.isearch.monitor.IsearchMonitor;
import com.tudou.isearch.monitor.Monitor;
import com.tudou.isearch.producer.SimpleProducer.SimpleModel;
import com.tudou.isearch.searcher.SearcherHandler;

public class Assets {
	public static final Analyzer STD_ANALYZER = new StandardAnalyzer();

	public static final long RAM_THRESHOLD_128KB = 128 * 1024;
	public static final long RAM_THRESHOLD_256KB = 256 * 1024;
	public static final long RAM_THRESHOLD_256MB = 256 * 1024 * 1024;
	public static final long RAM_THRESHOLD_128MB = 128 * 1024 * 1024;
	public static final long RAM_THRESHOLD_4MB = 4 * 1024 * 1024;
	public static final long RAM_THRESHOLD_16MB = 16 * 1024 * 1024;
	public static final long RAM_THRESHOLD_32MB = 32 * 1024 * 1024;

	public static final String SIMPLE_MODEL_800_x2_FILE_PATH = "/home/chenheng/Work/Tudou/isearch_test_data/itemId_800X2.txt";
	public static final String SIMPLE_MODEL_1W_x2_FILE_PATH = "/home/chenheng/Work/Tudou/isearch_test_data/itemId_1wX2.txt";
	public static final String SIMPLE_MODEL_2W_x2_FILE_PATH = "/home/chenheng/Work/Tudou/isearch_test_data/itemId_2wX2.txt";
	public static final String SIMPLE_MODEL_4W_x2_FILE_PATH = "/home/chenheng/Work/Tudou/isearch_test_data/itemId_4wX2.txt";
	public static final String SIMPLE_MODEL_5W_x2_FILE_PATH = "/home/chenheng/Work/Tudou/isearch_test_data/itemId_5wX2.txt";
	public static final String SIMPLE_MODEL_8W_x2_FILE_PATH = "/home/chenheng/Work/Tudou/isearch_test_data/itemId_8wX2.txt";
	public static final String SIMPLE_MODEL_10W_x2_FILE_PATH = "/home/chenheng/Work/Tudou/isearch_test_data/itemId_10wX2.txt";
	public static final String SIMPLE_MODEL_25W_x2_FILE_PATH = "/home/chenheng/Work/Tudou/isearch_test_data/itemId_25wX2.txt";
	public static final String SIMPLE_MODEL_50W_x2_FILE_PATH = "/home/chenheng/Work/Tudou/isearch_test_data/itemId_50wX2.txt";
	public static final String SIMPLE_MODEL_100W_x2_FILE_PATH = "/home/chenheng/Work/Tudou/isearch_test_data/itemId_100wX2.txt";

	public static final String BASE_PATH = "/tmp/isearch";
	public static final String SEGMENT_PATH = BASE_PATH;
	//public static final String CONTENT_FILE_PATH = "/home/chenheng/Work/Tudou/itemid20150122.txt";
	public static final String ASSEMBLED_INDEX_PATH = BASE_PATH
			+ "/assembled.index";

	public static final String[] HIT_ITEM_IDS = new String[] { "1032023",
			"3150576", "3150611", "3159332", "3203063", "3347620", "3347629",
			"3347644", "3347729", "3347794" };
	public static final String HIT_ITEM_ID = "1032023";

	public static String dynamicItemId() {
		return Math.random() > 0.5 ? "1032023" : "3347794";
	}

	public static Monitor MONITOR;
	public static LinkedBlockingQueue<SimpleModel> SIMPLE_BLOCKING_QUEUE_1W;
	public static LinkedBlockingQueue<SimpleModel> SIMPLE_BLOCKING_QUEUE_50W;
	public static LinkedBlockingQueue<SimpleModel> SIMPLE_BLOCKING_QUEUE_100W;
	public static LinkedBlockingQueue<SimpleModel> SIMPLE_BLOCKING_QUEUE_200W;
	public static SearcherHandler SEARCHER_MANAGER;
	

	public static void init() {
		MONITOR = new IsearchMonitor();
		SIMPLE_BLOCKING_QUEUE_1W = new LinkedBlockingQueue<SimpleModel>(10000);
		SIMPLE_BLOCKING_QUEUE_50W = new LinkedBlockingQueue<SimpleModel>(500000);
		SIMPLE_BLOCKING_QUEUE_100W = new LinkedBlockingQueue<SimpleModel>(
				1000000);
		SIMPLE_BLOCKING_QUEUE_200W = new LinkedBlockingQueue<SimpleModel>(
				2000000);
		SEARCHER_MANAGER = new SearcherHandler(Assets.STD_ANALYZER);
	}

	public static void reinit() {
		init();
	}

	public static void clear() {
		if (MONITOR != null) {
			MONITOR = new IsearchMonitor();
		}
		if (SIMPLE_BLOCKING_QUEUE_1W != null) {
			SIMPLE_BLOCKING_QUEUE_1W.clear();
		}
		if (SIMPLE_BLOCKING_QUEUE_50W != null) {
			SIMPLE_BLOCKING_QUEUE_50W.clear();
		}
		if (SIMPLE_BLOCKING_QUEUE_100W != null) {
			SIMPLE_BLOCKING_QUEUE_100W.clear();
		}
		if (SIMPLE_BLOCKING_QUEUE_200W != null) {
			SIMPLE_BLOCKING_QUEUE_200W.clear();
		}
		deleteDir(new File(BASE_PATH));
	}

	private static void deleteDir(File dir) {
		if (dir.isDirectory()) {
			for (File sd : dir.listFiles()) {
				deleteDir(sd);
			}
		}
		dir.deleteOnExit();
	}
	
	public static class SearchCommandWithDirectoryWrapper implements Runnable {
		private DirectoryWrapper directoryWrapper;
		private boolean searchAssembled = true;
		private boolean searchSegment = true;
		private boolean searchRam = true;
		private Map<String, Integer> hitMap;

		public SearchCommandWithDirectoryWrapper(DirectoryWrapper directoryWrapper,
				Map<String, Integer> hitMap) {
			this(directoryWrapper, hitMap, true, true, true);
		}

		public SearchCommandWithDirectoryWrapper(DirectoryWrapper directoryWrapper,
				Map<String, Integer> hitMap, boolean searchRam,
				boolean searchSegment, boolean searchAssembled) {
			this.directoryWrapper = directoryWrapper;
			this.hitMap = hitMap;
			this.searchRam = searchRam;
			this.searchSegment = searchSegment;
			this.searchAssembled = searchAssembled;
		}

		@Override
		public void run() {
			Set<Document> hitSet = Assets.SEARCHER_MANAGER.search("simpleId",
					Assets.HIT_ITEM_ID, directoryWrapper, searchRam, searchSegment,
					searchAssembled);
			Iterator<Document> it = hitSet.iterator();
			if (it.hasNext()) {
				Document doc = it.next();
				String itemId = doc.getField("simpleId").stringValue();
				if (hitMap.containsKey(itemId)) {
					hitMap.put(itemId, hitMap.get(itemId) + 1);
				} else {
					hitMap.put(itemId, 1);
				}
			}
		}
	}
	
	public static class SearchCommandWithReaderWrapper implements Runnable {
		private ReaderWrapper readerWrapper;
		private boolean searchAssembled = true;
		private boolean searchSegment = true;
		private boolean searchRam = true;
		private Map<String, Integer> hitMap;

		public SearchCommandWithReaderWrapper(ReaderWrapper readerWrapper,
				Map<String, Integer> hitMap) {
			this(readerWrapper, hitMap, true, true, true);
		}

		public SearchCommandWithReaderWrapper(ReaderWrapper readerWrapper,
				Map<String, Integer> hitMap, boolean searchRam,
				boolean searchSegment, boolean searchAssembled) {
			this.readerWrapper = readerWrapper;
			this.hitMap = hitMap;
			this.searchRam = searchRam;
			this.searchSegment = searchSegment;
			this.searchAssembled = searchAssembled;
		}

		@Override
		public void run() {
			Set<Document> hitSet = Assets.SEARCHER_MANAGER.search("simpleId",
					Assets.HIT_ITEM_ID, readerWrapper, searchRam, searchSegment,
					searchAssembled);
			Iterator<Document> it = hitSet.iterator();
			if (it.hasNext()) {
				Document doc = it.next();
				String itemId = doc.getField("simpleId").stringValue();
				if (hitMap.containsKey(itemId)) {
					hitMap.put(itemId, hitMap.get(itemId) + 1);
				} else {
					hitMap.put(itemId, 1);
				}
			}
		}
	}
}
