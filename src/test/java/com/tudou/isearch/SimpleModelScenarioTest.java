package com.tudou.isearch;

import java.io.IOException;
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
import com.tudou.isearch.indexer.RAMIndexer;
import com.tudou.isearch.producer.Producer;
import com.tudou.isearch.producer.SimpleProducer;
import com.tudou.isearch.producer.SimpleProducer.SimpleModel;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SimpleModelScenarioTest {

	public static final Logger logger = Logger
			.getLogger(SimpleModelScenarioTest.class);
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
		indexerPool = Executors.newSingleThreadExecutor();
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
	/**
	 * LBQ最大㖔吐量为50W时,只生产待索引数据。结果如下：<br>
	 * 1) 满队列大约需要消耗内存280MB;<br>
	 * 2) 耗时1.6秒;<br>
	 */
	public void test1FullQueneWith50W() {
		logger.info(">>>> [Test] begin to full Quene which accept 500,000 items...");
		
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_25W_x2_FILE_PATH, Assets.SIMPLE_BLOCKING_QUEUE_50W,
				Assets.MONITOR);
		producerPool.submit(producer);
		while (true) {
			if (Assets.SIMPLE_BLOCKING_QUEUE_50W.size() == 500000) {
				Runtime rt = Runtime.getRuntime();
				// Bytes to MB
				long totalMem = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
				logger.info("full quene with capacity 500,000 consume memory(MB):"
						+ totalMem);
				break;
			}
		}
		Assert.assertEquals(500000, Assets.SIMPLE_BLOCKING_QUEUE_50W.size());
	}

	@Test
	/**
	 * LBQ最大㖔吐量为100W时,只生产待索引数据。结果如下：<br>
	 * 1) 满队列大约需要消耗内存318MB;<br>
	 * 2) 耗时4.4秒;<br>
	 */
	public void test2FullQueneWith100W() {
		logger.info(">>>> [Test] begin to full Quene which accept 1,000,000 items...");
		
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_50W_x2_FILE_PATH, Assets.SIMPLE_BLOCKING_QUEUE_100W,
				Assets.MONITOR);
		producerPool.submit(producer);
		while (true) {
			if (Assets.SIMPLE_BLOCKING_QUEUE_100W.size() == 1000000) {
				Runtime rt = Runtime.getRuntime();
				// Bytes to MB
				long totalMem = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
				logger.info("full quene with capacity 1,000,000 consume memory(MB):"
						+ totalMem);
				break;
			}
		}
		Assert.assertEquals(1000000, Assets.SIMPLE_BLOCKING_QUEUE_100W.size());
	}

	@Test
	/**
	 * LBQ最大㖔吐量为200W时,只生产待索引数据。结果如下：<br>
	 * 1) 满队列大约需要消耗内存442MB;<br>
	 * 2) 耗时8.5秒;<br>
	 */
	public void test3FullQueneWith200W() {
		logger.info(">>>> [Test] begin to full Quene which accept 2,000,000 items...");
		
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_100W_x2_FILE_PATH, Assets.SIMPLE_BLOCKING_QUEUE_200W,
				Assets.MONITOR);
		producerPool.submit(producer);
		while (true) {
			if (Assets.SIMPLE_BLOCKING_QUEUE_200W.size() == 2000000) {
				Runtime rt = Runtime.getRuntime();
				// Bytes to MB
				long totalMem = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
				logger.info("full quene with capacity 2,000,000 consume memory(MB):"
						+ totalMem);
				break;
			}
		}
		Assert.assertEquals(2000000, Assets.SIMPLE_BLOCKING_QUEUE_200W.size());
	}

	@Test
	/**
	 * 16MB内存索引块，不断建立新的索引。如果如下：<br>
	 * 1) 大约可为9W条记录建立内存索引;<br>
	 * 2) 消耗时间约50秒.<br>
	 * @throws IOException
	 */
	public void test4FullRAMIndexderWith16MB() throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 16MB...");
		final IndexerManager<SimpleModel> iManager = new IndexerManager<SimpleModel>(
				Assets.ASSEMBLED_INDEX_PATH, Assets.STD_ANALYZER);
		
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_5W_x2_FILE_PATH, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.MONITOR);
		producerPool.submit(producer);
		
		RAMIndexer<SimpleModel> indexer = new RAMIndexer<SimpleModel>(
				"test4FullRAMIndexderWith16MB", Assets.STD_ANALYZER,
				Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.RAM_THRESHOLD_16MB,
				Assets.SEGMENT_PATH, Assets.MONITOR);
		iManager.registerRAMIndexer(indexer);
		indexerPool.submit(indexer);

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() >= 85000) {
				Assets.MONITOR.summary();
				Assert.assertTrue(indexer.getDirectory().ramBytesUsed() < Assets.RAM_THRESHOLD_16MB);
				logger.info(">>>> consume memory(MB): "
						+ indexer.getDirectory().ramBytesUsed() / 1024 / 1024);
				break;
			}
		}
	}

	@Test
	/**
	 * 32MB内存索引块，不断建立新的索引。如果如下：<br>
	 * 1) 大约可为18W条记录建立内存索引;<br>
	 * 2) 消耗时间约177秒.<br>
	 * @throws IOException
	 */
	public void test5FullRAMIndexderWith32MB() throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 32MB...");
		final IndexerManager<SimpleModel> iManager = new IndexerManager<SimpleModel>(
				Assets.ASSEMBLED_INDEX_PATH, Assets.STD_ANALYZER);
		
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_10W_x2_FILE_PATH, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.MONITOR);
		producerPool.submit(producer);

		RAMIndexer<SimpleModel> indexer = new RAMIndexer<SimpleModel>(
				"test5FullRAMIndexderWith32MB", Assets.STD_ANALYZER,
				Assets.SIMPLE_BLOCKING_QUEUE_1W, Assets.RAM_THRESHOLD_32MB,
				Assets.SEGMENT_PATH, Assets.MONITOR);
		iManager.registerRAMIndexer(indexer);
		indexerPool.submit(indexer);

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() >= 170000) {
				Assets.MONITOR.summary();
				Assert.assertTrue(indexer.getDirectory().ramBytesUsed() < Assets.RAM_THRESHOLD_32MB);
				logger.info(">>>> consume memory(MB): "
						+ indexer.getDirectory().ramBytesUsed() / 1024 / 1024);
				break;
			}
		}
	}

	/**
	 * 16MB内存块，在不断写入索引数据的同时不断的查询命中特定的ID数据。结果如下:<br>
	 * 1) 建立内存索引数据80000条；<br>
	 * 2) 使用JVM内存共100MB;<br>
	 * 3) 内存索引块共消耗10MB；<br>
	 * 4) 查询ID均命中170180次；<br>
	 * 5) 共消耗时间127秒;<br>
	 * 
	 * @throws IOException
	 */
	@Test
	public void test6FillRamIndexer4MbAndSearchHitInRam() throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 4MB...");

		final IndexerManager<SimpleModel> iManager = new IndexerManager<SimpleModel>(
				Assets.ASSEMBLED_INDEX_PATH, Assets.STD_ANALYZER);
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_2W_x2_FILE_PATH, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.MONITOR);
		producerPool.submit(producer);

		RAMIndexer<SimpleModel> indexer = new RAMIndexer<SimpleModel>(
				"test6FillRamIndexer4MbAndSearchHitInRam",
				Assets.STD_ANALYZER, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.RAM_THRESHOLD_4MB, Assets.SEGMENT_PATH, Assets.MONITOR);
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

	/**
	 * 16MB内存块，在不断写入索引数据的同时不断的查询命中特定的ID数据。结果如下:<br>
	 * 1) 建立内存索引数据80000条；<br>
	 * 2) 使用JVM内存共109MB;<br>
	 * 3) 内存索引块共消耗14MB；<br>
	 * 4) 查询ID均命中105578次；<br>
	 * 5) 共消耗时间86秒;<br>
	 * 
	 * @throws IOException
	 */
	@Test
	public void test6_1FillRamIndexer4MbAndSearchHitInRam() throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 4MB...");
		final IndexerManager<SimpleModel> iManager = new IndexerManager<SimpleModel>(Assets.ASSEMBLED_INDEX_PATH,
				Assets.STD_ANALYZER);
		
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_2W_x2_FILE_PATH, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.MONITOR);
		producerPool.submit(producer);
		
		RAMIndexer<SimpleModel> indexer = new RAMIndexer<SimpleModel>(
				"test6_1FillRamIndexer4MbAndSearchHitInRam",
				Assets.STD_ANALYZER, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.RAM_THRESHOLD_4MB, Assets.SEGMENT_PATH, Assets.MONITOR);
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

	/**
	 * 16MB内存块，在不断写入索引数据的同时不断的查询命中特定的ID数据；<br>
	 * 并触发至少一次文件块索引dump，以使后继查询到文件块命中。结果如下:<br>
	 * 1) 建立内存索引及文件块索引数据140000条；<br>
	 * 2) 使用JVM内存共256MB;<br>
	 * 3) 内存索引块共消耗9MB(dump后新分配的内存块)；<br>
	 * 4) 查询ID均命中195060次；<br>
	 * 5) 共消耗时间223秒;<br>
	 * 
	 * @throws IOException
	 */
	//@Test
	public void test7FillRamIndexer4MbAndSearchHitInBothRamAndFs()
			throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 4MB...");
		final IndexerManager<SimpleModel> iManager = new IndexerManager<SimpleModel>(Assets.ASSEMBLED_INDEX_PATH,
				Assets.STD_ANALYZER);
		
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_4W_x2_FILE_PATH, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.MONITOR);
		producerPool.submit(producer);

		
		RAMIndexer<SimpleModel> indexer = new RAMIndexer<SimpleModel>(
				"test7FillRamIndexer16MbAndSearchHitInBothRamAndFs",
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

	/**
	 * 16MB内存块，在不断写入索引数据的同时不断的查询命中特定的ID数据；<br>
	 * 并触发至少一次文件块索引dump，以使后继查询到文件块命中。结果如下:<br>
	 * 1) 建立内存索引及文件块索引数据140000条；<br>
	 * 2) 使用JVM内存共256MB;<br>
	 * 3) 内存索引块共消耗9MB(dump后新分配的内存块)；<br>
	 * 4) 查询ID均命中4793次；<br>
	 * 5) 共消耗时间98秒;<br>
	 * 
	 * @throws IOException
	 */
	//@Test
	public void test7_1FillRamIndexer16MbAndSearchHitInBothRamAndFs()
			throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 16MB...");
		final IndexerManager<SimpleModel> iManager = new IndexerManager<SimpleModel>(Assets.ASSEMBLED_INDEX_PATH,
				Assets.STD_ANALYZER);
		
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_10W_x2_FILE_PATH, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.MONITOR);
		producerPool.submit(producer);
		
		RAMIndexer<SimpleModel> indexer = new RAMIndexer<SimpleModel>(
				"test7_1FillRamIndexer16MbAndSearchHitInBothRamAndFs",
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
	public void test8FillRamIndexer256KbAndSearchHitInBothRamAndFs()
			throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 256KB...");
		final IndexerManager<SimpleModel> iManager = new IndexerManager<SimpleModel>(Assets.ASSEMBLED_INDEX_PATH,
				Assets.STD_ANALYZER);
		
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_1W_x2_FILE_PATH, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.MONITOR);
		producerPool.submit(producer);
		
		RAMIndexer<SimpleModel> indexer = new RAMIndexer<SimpleModel>(
				"test8FillRamIndexer256KbAndSearchHitInBothRamAndFs",
				Assets.STD_ANALYZER, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.RAM_THRESHOLD_256KB, Assets.SEGMENT_PATH, Assets.MONITOR);
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
	public void test8_1FillRamIndexer256KbAndSearchHitInBothRamAndFs()
			throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 256KB...");
		final IndexerManager<SimpleModel> iManager = new IndexerManager<SimpleModel>(Assets.ASSEMBLED_INDEX_PATH,
				Assets.STD_ANALYZER);
		
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_1W_x2_FILE_PATH, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.MONITOR);
		producerPool.submit(producer);

		
		RAMIndexer<SimpleModel> indexer = new RAMIndexer<SimpleModel>(
				"test8_1FillRamIndexer256KbAndSearchHitInBothRamAndFs",
				Assets.STD_ANALYZER, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.RAM_THRESHOLD_256KB, Assets.SEGMENT_PATH, Assets.MONITOR);
		iManager.registerRAMIndexer(indexer);
		indexerPool.submit(indexer);

		Map<String, Integer> hitMap = new ConcurrentHashMap<String, Integer>();

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 0) {
				searcherScheduleWithReaderWrapper(iManager, hitMap, true,
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
	public void test8_2FillRamIndexer4MbAndSearchHitInBothRamAndFs()
			throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 4MB...");
		final IndexerManager<SimpleModel> iManager = new IndexerManager<SimpleModel>(Assets.ASSEMBLED_INDEX_PATH,
				Assets.STD_ANALYZER);
		
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_4W_x2_FILE_PATH, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.MONITOR);
		producerPool.submit(producer);

		
		RAMIndexer<SimpleModel> indexer = new RAMIndexer<SimpleModel>(
				"test8_2FillRamIndexer4MbAndSearchHitInBothRamAndFs",
				Assets.STD_ANALYZER, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.RAM_THRESHOLD_4MB, Assets.SEGMENT_PATH, Assets.MONITOR);
		iManager.registerRAMIndexer(indexer);
		indexerPool.submit(indexer);

		Map<String, Integer> hitMap = new ConcurrentHashMap<String, Integer>();

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 0) {
				searcherScheduleWithReaderWrapper(iManager, hitMap, true,
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
				+ indexer.getDirectory().ramBytesUsed() / 1024/1024);
		for (Entry<String, Integer> entry : hitMap.entrySet()) {
			logger.info(">>>> Hit document(" + entry.getKey() + ") "
					+ entry.getValue() + " times");
		}
	}
	
	@Test
	public void test8_3FillRamIndexer4MbAndSearchHitInBothRamAndFs()
			throws IOException {
		logger.info(">>>> [Test] begin to full RAM Indexer which is 4MB...");
		final IndexerManager<SimpleModel> iManager = new IndexerManager<SimpleModel>(Assets.ASSEMBLED_INDEX_PATH,
				Assets.STD_ANALYZER);
		
		Producer<SimpleModel> producer = new SimpleProducer(
				Assets.SIMPLE_MODEL_4W_x2_FILE_PATH, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.MONITOR);
		producerPool.submit(producer);

		
		RAMIndexer<SimpleModel> indexer = new RAMIndexer<SimpleModel>(
				"test8_2FillRamIndexer4MbAndSearchHitInBothRamAndFs",
				Assets.STD_ANALYZER, Assets.SIMPLE_BLOCKING_QUEUE_1W,
				Assets.RAM_THRESHOLD_4MB, Assets.SEGMENT_PATH, Assets.MONITOR);
		iManager.registerRAMIndexer(indexer);
		indexerPool.submit(indexer);

		Map<String, Integer> hitMap = new ConcurrentHashMap<String, Integer>();

		while (true) {
			if (Assets.MONITOR.getTotalConsumption() > 0) {
				searcherScheduleWithReaderWrapper(iManager, hitMap, true,
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
				+ indexer.getDirectory().ramBytesUsed() / 1024/1024);
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
			final IndexerManager<SimpleModel> iManager, final Map<String, Integer> hitMap,
			final boolean searchRam, final boolean searchSegment,
			final boolean searchAssembled) {
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
			final IndexerManager<SimpleModel> iManager, final Map<String, Integer> hitMap,
			final boolean searchRam, final boolean searchSegment,
			final boolean searchAssembled) {
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