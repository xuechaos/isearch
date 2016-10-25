package com.tudou.isearch.producer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;

import com.tudou.isearch.Model;
import com.tudou.isearch.monitor.Monitor;
import com.tudou.isearch.producer.SimpleProducer.SimpleModel;

public class SimpleProducer extends AbstractProducer<SimpleModel> {

	private final static Logger logger = Logger.getLogger(SimpleProducer.class);
	/**
	 * 待消费文件路径
	 */
	private String contentPath;
	/**
	 * 生产数据中间缓冲队列；解决生产数据与消费数据不匹配的问题。<br>
	 * 目标是能快速生产快速消费.
	 */
	private final LinkedBlockingQueue<SimpleModel> queue;

	/**
	 * 生产者，用于生产待索引的数据
	 * 
	 * @param contentPath
	 *            待消费文件路径
	 * @param queue
	 *            生产数据中间缓冲队列；解决生产数据与消费数据不匹配的问题。目标是能快速生产快速消费
	 */
	public SimpleProducer(String contentPath,
			LinkedBlockingQueue<SimpleModel> queue, Monitor monitor
			) {
		super(monitor);
		this.contentPath = contentPath;
		this.queue = queue;
	}

	/**
	 * 生产待索引数据，并放入中间队列中等待消费
	 */
	@Override
	public SimpleModel produce() {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(this.contentPath)));
			String line;
			while ((line = reader.readLine()) != null) {
				// 从文件中读出Id, 产生uuid做为描述内容
				SimpleModel m = new SimpleModel(line, UUID.randomUUID()
						.toString());
				this.queue.put(m);
				logger.debug(">>>> produce content(" + m
						+ ") to blockling quene...");
				this.monitor.produceEvent();
			}
			logger.info(">>>> simple models procude finished...");
		} catch (IOException | InterruptedException e) {
			logger.error("!!!! IO Exception", e);
			return null;
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					logger.error("!!!! IO Exception", e);
				}
			}
		}
		return null;
	}

	/**
	 * 简单数据模型
	 * 
	 * @author chenheng
	 * 
	 */
	public class SimpleModel extends Model {
		private String id;
		private String desc;

		public SimpleModel(String id, String desc) {
			this.id = id;
			this.desc = desc;
		}

		/**
		 * 为简单数据建立索引规则
		 * 
		 * @param doc
		 */
		@Override
		public void toDocument(Document doc) {
			doc.add(new StringField(getUniqueKeyName(), id, Field.Store.YES));
			doc.add(new StringField("desc", desc, Field.Store.YES));
		}

		@Override
		public String getUniqueKeyValue() {
			return id;
		}

		@Override
		public String getUniqueKeyName() {
			return "simpleId";
		}
	}
}
