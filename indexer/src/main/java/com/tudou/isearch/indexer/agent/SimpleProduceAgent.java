package com.tudou.isearch.indexer.agent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.tudou.isearch.common.SimpleModel;

//@Service("simpleProduceAgent")
public class SimpleProduceAgent implements ProduceAgent<SimpleModel> {
	private final static Logger logger = Logger.getLogger(SimpleProduceAgent.class);
	/**
	 * 生产数据中间缓冲队列；解决生产数据与消费数据不匹配的问题。<br>
	 * 目标是能快速生产快速消费.
	 */
	@Resource
	SimpleMonitor monitor;

	//@Resource(name="simpleQueue")
	private LinkedBlockingQueue<SimpleModel> simpleQueue;

	/**
	 * 生产待索引数据，并放入中间队列中等待消费
	 */
	@Override
	public SimpleModel produce(SimpleModel model) {
		if (this.simpleQueue.offer(model)) {
			this.monitor.produceEvent();
			logger.debug(">>>> produce content(" + model
					+ ") to blockling quene");
			return model;
		}
		return null;
	}

	public Map<String, String> status() {
		return monitor.summary();
	}

	public SimpleMonitor getMonitor() {
		return monitor;
	}

	public void setMonitor(SimpleMonitor monitor) {
		this.monitor = monitor;
	}
	
	public LinkedBlockingQueue<SimpleModel> getSimpleQueue() {
		return simpleQueue;
	}

	public void setSimpleQueue(LinkedBlockingQueue<SimpleModel> simpleQueue) {
		this.simpleQueue = simpleQueue;
	}

	public static class SimpleMonitor {
		private AtomicLong totalProduction;

		public SimpleMonitor() {
			totalProduction = new AtomicLong();
		}

		public void produceEvent() {
			totalProduction.incrementAndGet();
		}

		public Map<String, String> summary() {
			Map<String, String> map = new HashMap<String, String>();
			map.put("production", String.valueOf(totalProduction.longValue()));
			return map;
		}
	}
}
