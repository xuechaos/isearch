package com.tudou.isearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.tudou.isearch.Assets.SearchCommandWithDirectoryWrapper;
import com.tudou.isearch.Assets.SearchCommandWithReaderWrapper;
import com.tudou.isearch.indexer.IndexerManager;
import com.tudou.isearch.indexer.IndexerManager2;
import com.tudou.isearch.indexer.Manager;
import com.tudou.isearch.indexer.RAMIndexer;
import com.tudou.isearch.indexer.RAMIndexer2;
import com.tudou.isearch.indexer.RAMIndexer3;
import com.tudou.isearch.producer.Producer;
import com.tudou.isearch.producer.SimpleProducer;
import com.tudou.isearch.producer.SimpleProducer.SimpleModel;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SimpleModelScenarioTest3 {

	public static final Logger logger = Logger
			.getLogger(SimpleModelScenarioTest2.class);
	private static ExecutorService producerPool;
	private static ExecutorService indexerPool;
	private static ExecutorService searcherPool;
	private static ScheduledExecutorService searcherSchedule;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Assets.init();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		Assets.clear();
	}

	@Before
	public void setUp() {
		producerPool = Executors.newSingleThreadExecutor();
		indexerPool = Executors.newCachedThreadPool();
		searcherPool = Executors.newFixedThreadPool(20);// 建议最大20个线程.经测试,超过20个线程后,影响也不再明显.
		searcherSchedule = Executors.newSingleThreadScheduledExecutor();
	}

	@After
	public void tearDown() {
		producerPool = indexerPool = searcherPool = null;
		searcherSchedule = null;
		Assets.clear();
	}

	@Test
	public void test1FullRAMIndexderWith16MB() throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 16MB...");
		final IndexerManager2<SimpleModel> iManager = new IndexerManager2<SimpleModel>(
				Assets.ASSEMBLED_INDEX_PATH, Assets.STD_ANALYZER);

		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_5W_x2_FILE_PATH,
				Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.MONITOR);
		producerPool.submit(producer);
		List<RAMIndexer3<SimpleModel>> indexerList = new ArrayList<RAMIndexer3<SimpleModel>>();
		for (int i = 0; i < 1; i++) {
			RAMIndexer3<SimpleModel> indexer = new RAMIndexer3<SimpleModel>(
					""+i, Assets.STD_ANALYZER,
					Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.RAM_THRESHOLD_16MB,
					Assets.SEGMENT_PATH, Assets.MONITOR,
					iManager.getAssembledDeleteQueue());
			iManager.registerRAMIndexer(indexer);
			indexerList.add(indexer);
			indexerPool.submit(indexer);
		}

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() >= 85000) {
				Assets.MONITOR.summary();
				//Assert.assertTrue(indexer.getDirectory().ramBytesUsed() < Assets.RAM_THRESHOLD_16MB);
				//logger.info(">>>> consume memory(MB): "
				//		+ indexer.getDirectory().ramBytesUsed() / 1024 / 1024);
				for (int i= 0;i<indexerList.size();i++) {
					logger.info(">>>>>indexerList.get(" + i
							+ "): consume memory(MB):"
							+ indexerList.get(i).getDirectory().ramBytesUsed());
				}
				break;
			}
		}
	}

	@Test
	public void test2FullRAMIndexderWith32MB() throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 32MB...");
		final IndexerManager2<SimpleModel> iManager = new IndexerManager2<SimpleModel>(
				Assets.ASSEMBLED_INDEX_PATH, Assets.STD_ANALYZER);

		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_10W_x2_FILE_PATH,
				Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.MONITOR);
		producerPool.submit(producer);
		for (int i = 0; i < 10; i++) {
		RAMIndexer2<SimpleModel> indexer = new RAMIndexer2<SimpleModel>(
				"test2FullRAMIndexderWith32MB"+i, Assets.STD_ANALYZER,
				Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.RAM_THRESHOLD_32MB,
				Assets.SEGMENT_PATH, Assets.MONITOR,
				iManager.getAssembledDeleteQueue());
		iManager.registerRAMIndexer(indexer);
		indexerPool.submit(indexer);
		}

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() >= 170000) {
				Assets.MONITOR.summary();
				//Assert.assertTrue(indexer.getDirectory().ramBytesUsed() < Assets.RAM_THRESHOLD_32MB);
				//logger.info(">>>> consume memory(MB): "
				//		+ indexer.getDirectory().ramBytesUsed() / 1024 / 1024);
				break;
			}
		}
	}

	@Test
	public void test3FillRamIndexer4MbAndSearchHitInRam() throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 4MB...");

		final IndexerManager2<SimpleModel> iManager = new IndexerManager2<SimpleModel>(
				Assets.ASSEMBLED_INDEX_PATH, Assets.STD_ANALYZER);
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_2W_x2_FILE_PATH,
				Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.MONITOR);
		producerPool.submit(producer);

		RAMIndexer2<SimpleModel> indexer = new RAMIndexer2<SimpleModel>(
				"test3FillRamIndexer4MbAndSearchHitInRam", Assets.STD_ANALYZER,
				Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.RAM_THRESHOLD_4MB,
				Assets.SEGMENT_PATH, Assets.MONITOR,
				iManager.getAssembledDeleteQueue());
		iManager.registerRAMIndexer(indexer);
		indexerPool.submit(indexer);

		Map<String, Integer> hitMap = new ConcurrentHashMap<String, Integer>();

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 0) {
				searcherScheduleWithDirectoryWrapper(iManager, hitMap, true,
						false, false);
				break;
			}
		}

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 20000) { // 此时索引内存即将填满
				Assert.assertTrue(indexer.getDirectory().ramBytesUsed() < Assets.RAM_THRESHOLD_4MB);
				stop();
				break;
			}
		}
		Assets.MONITOR.summary();
		logger.info(">>>> consume memory(MB): "
				+ indexer.getDirectory().ramBytesUsed() / 1024 / 1024);
		for (Entry<String, Integer> entry : hitMap.entrySet()) {
			logger.info(">>>> Hit document(" + entry.getKey() + ") "
					+ entry.getValue() + " times");
		}
	}

	@Test
	public void test3_1FillRamIndexer4MbAndSearchHitInRam() throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 4MB...");
		final IndexerManager2<SimpleModel> iManager = new IndexerManager2<SimpleModel>(
				Assets.ASSEMBLED_INDEX_PATH, Assets.STD_ANALYZER);

		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_2W_x2_FILE_PATH,
				Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.MONITOR);
		producerPool.submit(producer);

		RAMIndexer2<SimpleModel> indexer = new RAMIndexer2<SimpleModel>(
				"test3_1FillRamIndexer4MbAndSearchHitInRam",
				Assets.STD_ANALYZER, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.RAM_THRESHOLD_4MB, Assets.SEGMENT_PATH, Assets.MONITOR,
				iManager.getAssembledDeleteQueue());
		iManager.registerRAMIndexer(indexer);
		indexerPool.submit(indexer);

		Map<String, Integer> hitMap = new ConcurrentHashMap<String, Integer>();
		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 0) {
				searcherScheduleWithReaderWrapper(iManager, hitMap, true,
						false, false);
				break;
			}
		}
		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 20000) { // 此时索引内存即将填满
				Assert.assertTrue(indexer.getDirectory().ramBytesUsed() < Assets.RAM_THRESHOLD_4MB);
				stop();
				break;
			}
		}
		Assets.MONITOR.summary();
		logger.info(">>>> consume memory(MB): "
				+ indexer.getDirectory().ramBytesUsed() / 1024 / 1024);
		for (Entry<String, Integer> entry : hitMap.entrySet()) {
			logger.info(">>>> Hit document(" + entry.getKey() + ") "
					+ entry.getValue() + " times");
		}
	}

	// @Test
	public void test4FillRamIndexer4MbAndSearchHitInBothRamAndFs()
			throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 4MB...");
		final IndexerManager<SimpleModel> iManager = new IndexerManager<SimpleModel>(
				Assets.ASSEMBLED_INDEX_PATH, Assets.STD_ANALYZER);

		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_4W_x2_FILE_PATH,
				Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.MONITOR);
		producerPool.submit(producer);

		RAMIndexer<SimpleModel> indexer = new RAMIndexer<SimpleModel>(
				"test4FillRamIndexer4MbAndSearchHitInBothRamAndFs",
				Assets.STD_ANALYZER, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.RAM_THRESHOLD_4MB, Assets.SEGMENT_PATH, Assets.MONITOR);
		iManager.registerRAMIndexer(indexer);
		indexerPool.submit(indexer);

		Map<String, Integer> hitMap = new ConcurrentHashMap<String, Integer>();

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 0) {
				searcherScheduleWithDirectoryWrapper(iManager, hitMap, true,
						true, false);
				break;
			}
		}

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 50000) { // 此时索引内存将填满并完成一次文件块dump
				Assert.assertTrue(indexer.getDirectory().ramBytesUsed() < Assets.RAM_THRESHOLD_4MB);
				stop();
				break;
			}
		}
		Assets.MONITOR.summary();
		logger.info(">>>> consume memory(MB): "
				+ indexer.getDirectory().ramBytesUsed() / 1024 / 1024);
		for (Entry<String, Integer> entry : hitMap.entrySet()) {
			logger.info(">>>> Hit document(" + entry.getKey() + ") "
					+ entry.getValue() + " times");
		}
	}

	// @Test
	public void test4_1FillRamIndexer16MbAndSearchHitInBothRamAndFs()
			throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 16MB...");
		final IndexerManager<SimpleModel> iManager = new IndexerManager<SimpleModel>(
				Assets.ASSEMBLED_INDEX_PATH, Assets.STD_ANALYZER);

		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_10W_x2_FILE_PATH,
				Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.MONITOR);
		producerPool.submit(producer);

		RAMIndexer<SimpleModel> indexer = new RAMIndexer<SimpleModel>(
				"test4_1FillRamIndexer16MbAndSearchHitInBothRamAndFs",
				Assets.STD_ANALYZER, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.RAM_THRESHOLD_16MB, Assets.SEGMENT_PATH, Assets.MONITOR);
		iManager.registerRAMIndexer(indexer);
		indexerPool.submit(indexer);

		Map<String, Integer> hitMap = new ConcurrentHashMap<String, Integer>();
		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 0) {
				searcherScheduleWithReaderWrapper(iManager, hitMap, true, true,
						false);
				break;
			}
		}

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 140000) { // 此时索引内存将填满并完成一次文件块dump
				Assert.assertTrue(indexer.getDirectory().ramBytesUsed() < Assets.RAM_THRESHOLD_16MB);
				stop();
				break;
			}
		}
		Assets.MONITOR.summary();
		logger.info(">>>> consume memory(MB): "
				+ indexer.getDirectory().ramBytesUsed() / 1024 / 1024);
		for (Entry<String, Integer> entry : hitMap.entrySet()) {
			logger.info(">>>> Hit document(" + entry.getKey() + ") "
					+ entry.getValue() + " times");
		}
	}

	@Test
	public void test5FillRamIndexer256KbAndSearchHitInBothRamAndFs()
			throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 256KB...");
		final IndexerManager2<SimpleModel> iManager = new IndexerManager2<SimpleModel>(
				null, Assets.STD_ANALYZER);

		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_1W_x2_FILE_PATH,
				Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.MONITOR);
		producerPool.submit(producer);

		RAMIndexer2<SimpleModel> indexer = new RAMIndexer2<SimpleModel>(
				"test5FillRamIndexer256KbAndSearchHitInBothRamAndFs",
				Assets.STD_ANALYZER, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.RAM_THRESHOLD_256KB, Assets.SEGMENT_PATH,
				Assets.MONITOR, iManager.getAssembledDeleteQueue());
		iManager.registerRAMIndexer(indexer);
		indexerPool.submit(indexer);

		Map<String, Integer> hitMap = new ConcurrentHashMap<String, Integer>();

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 0) {
				searcherScheduleWithDirectoryWrapper(iManager, hitMap, true,
						true, false);
				break;
			}
		}

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 7000) { // 此时索引内存将填满并完成一次文件块dump
				Assert.assertTrue(indexer.getDirectory().ramBytesUsed() < Assets.RAM_THRESHOLD_256KB);
				stop();
				break;
			}
		}
		Assets.MONITOR.summary();
		logger.info(">>>> consume memory(KB): "
				+ indexer.getDirectory().ramBytesUsed() / 1024);
		for (Entry<String, Integer> entry : hitMap.entrySet()) {
			logger.info(">>>> Hit document(" + entry.getKey() + ") "
					+ entry.getValue() + " times");
		}
	}

	@Test
	public void test5_1FillRamIndexer256KbAndSearchHitInBothRamAndFs()
			throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 256KB...");
		final IndexerManager2<SimpleModel> iManager = new IndexerManager2<SimpleModel>(
				null, Assets.STD_ANALYZER);

		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_1W_x2_FILE_PATH,
				Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.MONITOR);
		producerPool.submit(producer);

		RAMIndexer2<SimpleModel> indexer = new RAMIndexer2<SimpleModel>(
				"test5_1FillRamIndexer256KbAndSearchHitInBothRamAndFs",
				Assets.STD_ANALYZER, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.RAM_THRESHOLD_256KB, Assets.SEGMENT_PATH,
				Assets.MONITOR, iManager.getAssembledDeleteQueue());
		iManager.registerRAMIndexer(indexer);
		indexerPool.submit(indexer);

		Map<String, Integer> hitMap = new ConcurrentHashMap<String, Integer>();

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 0) {
				searcherScheduleWithReaderWrapper(iManager, hitMap, true, true,
						false);
				break;
			}
		}

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 7000) { // 此时索引内存将填满并完成一次文件块dump
				Assert.assertTrue(indexer.getDirectory().ramBytesUsed() < Assets.RAM_THRESHOLD_256KB);
				stop();
				break;
			}
		}
		Assets.MONITOR.summary();
		logger.info(">>>> consume memory(KB): "
				+ indexer.getDirectory().ramBytesUsed() / 1024);
		for (Entry<String, Integer> entry : hitMap.entrySet()) {
			logger.info(">>>> Hit document(" + entry.getKey() + ") "
					+ entry.getValue() + " times");
		}
	}

	@Test
	public void test6FillRamIndexer4MbAndSearchHitInBothRamAndFs()
			throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 4MB...");
		final IndexerManager2<SimpleModel> iManager = new IndexerManager2<SimpleModel>(
				null, Assets.STD_ANALYZER);

		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_4W_x2_FILE_PATH,
				Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.MONITOR);
		producerPool.submit(producer);

		RAMIndexer2<SimpleModel> indexer = new RAMIndexer2<SimpleModel>(
				"test6_2FillRamIndexer4MbAndSearchHitInBothRamAndFs",
				Assets.STD_ANALYZER, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.RAM_THRESHOLD_4MB, Assets.SEGMENT_PATH, Assets.MONITOR,
				iManager.getAssembledDeleteQueue());
		iManager.registerRAMIndexer(indexer);
		indexerPool.submit(indexer);

		Map<String, Integer> hitMap = new ConcurrentHashMap<String, Integer>();

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 0) {
				searcherScheduleWithReaderWrapper(iManager, hitMap, true, true,
						false);
				break;
			}
		}

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 50000) { // 此时索引内存将填满并完成一次文件块dump
				Assert.assertTrue(indexer.getDirectory().ramBytesUsed() < Assets.RAM_THRESHOLD_4MB);
				stop();
				break;
			}
		}
		Assets.MONITOR.summary();
		logger.info(">>>> consume memory(MB): "
				+ indexer.getDirectory().ramBytesUsed() / 1024 / 1024);
		for (Entry<String, Integer> entry : hitMap.entrySet()) {
			logger.info(">>>> Hit document(" + entry.getKey() + ") "
					+ entry.getValue() + " times");
		}
	}

	private void stop() {
		searcherPool.shutdownNow();
		indexerPool.shutdownNow();
		producerPool.shutdownNow();
		searcherSchedule.shutdownNow();
	}

	private void searcherScheduleWithDirectoryWrapper(
			final Manager<SimpleModel> iManager,
			final Map<String, Integer> hitMap, final boolean searchRam,
			final boolean searchSegment, final boolean searchAssembled) {
		searcherSchedule.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				SearchCommandWithDirectoryWrapper command = new SearchCommandWithDirectoryWrapper(
						iManager.getDirectoryWrapper(), hitMap, searchRam,
						searchSegment, searchAssembled);
				searcherPool.submit(command);
			}
		}, 1000, 750, TimeUnit.MICROSECONDS);
	}

	private void searcherScheduleWithReaderWrapper(
			final Manager<SimpleModel> iManager,
			final Map<String, Integer> hitMap, final boolean searchRam,
			final boolean searchSegment, final boolean searchAssembled) {
		searcherSchedule.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				SearchCommandWithReaderWrapper command = new SearchCommandWithReaderWrapper(
						iManager.getReaderWrapper(), hitMap, searchRam,
						searchSegment, searchAssembled);
				searcherPool.submit(command);
			}
		}, 1000, 750, TimeUnit.MICROSECONDS);
	}
}